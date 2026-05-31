"""
DAILY SYNC — Monday (source of truth) → HubSpot

Modes:
  --incremental   Only process items changed since last run (steps 3-6).
                  Steps 1-2 are skipped (structural, run daily).
                  Designed for high-frequency execution (every 10 min).
  (default)       Full run — all 6 steps, process entire base.

Steps:
  1. Refresh ignition_all_quote_ids on Ventas deals  (full-run only)
  2. Create new HubSpot Quote objects for missing PROP-XXX  (full-run only)
  3. Update existing quote titles when status changes in Monday Propuestas
  4. Sync money + closedate on Ventas deals (from Monday Propuestas)
  5. Sync money + closedate on Upsells deals (from Monday Upselling)
  6. Sync dealstage on Ventas deals (HS dealstage = Monday Oportunidad status)

Designed to be idempotent — only writes when values would actually change.
Safe to re-run at any frequency.
"""
import json, sys, csv, re, time, urllib.request, urllib.error, argparse, os, fcntl
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
csv.field_size_limit(sys.maxsize)

HS_H = {"Authorization":"Bearer pat-na2-e00f5c8e-cd02-4ec6-95db-8b733677335b","Content-Type":"application/json"}
MONDAY_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjY1MzEwODg2MSwiYWFpIjoxMSwidWlkIjoxMDE3NDk2MzEsImlhZCI6IjIwMjYtMDUtMDNUMTM6MDA6NDkuMDAwWiIsInBlciI6Im1lOndyaXRlIiwiYWN0aWQiOjE0NTQ2ODI1LCJyZ24iOiJ1c2UxIn0.GCnjKZ1VqvdiQFIxPKsjIm1Q06hF1ytPaUGqC2IxcE4"
MH = {"Authorization": MONDAY_TOKEN, "Content-Type": "application/json", "API-Version": "2024-01"}

VENTAS_PIPELINE = "2250405610"
UPSELLS_PIPELINE = "2269182681"
OPORTUNIDADES_BOARD = "5435285342"
PROPUESTAS_BOARD = "5435295373"
UPSELLING_BOARD = "5744094861"
PROP_RE = re.compile(r'\bPROP-\d+', re.IGNORECASE)
IGN_CSV = "/Users/marcosotomayor/Downloads/lazo-us-Pipeline-2026-05-28T170501Z.csv"
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".quote_money_sync_state.json")
LOCK_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".quote_money_sync.lock")

# Monday Oportunidades status → HS Ventas dealstage_id
STAGE_MAP = {
    'Meeting Scheduled':   '3617654468',
    'Reschedule':          '3617654469',
    'Cancelled':           '3617654470',
    'Disqualified':        '3617654471',
    'Create Quote':        '3617654472',
    'Awaiting Acceptance': '3687871166',
    'Next Month':          '3617654474',
    'Future':              '3617654475',
    'In Review':           '3617654476',
    'No Answer':           '3617654477',
    'Closed Won':          '3617654478',
    'Closed Lost':         '3617654479',
    'No Show':             '3754569417',
}

# Propuesta state → title tag for quote
STATE_TAG = {
    "Accepted":           "ACCEPTED",
    "Completed":          "COMPLETED",
    "Lost":               "REJECTED",
    "Awaiting acceptance":"AWAITING",
    "Draft":              "DRAFT",
    "Review":             "REVIEW",
}


def hs(method, path, body=None, retries=3):
    for a in range(retries):
        try:
            req=urllib.request.Request(f'https://api.hubapi.com{path}', data=json.dumps(body).encode() if body else None, headers=HS_H, method=method)
            with urllib.request.urlopen(req, timeout=30) as r: return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429,502,503,504) and a<retries-1: time.sleep(5); continue
            return e.code, json.loads(e.read())


def monday(q, retries=3):
    for a in range(retries):
        try:
            req=urllib.request.Request('https://api.monday.com/v2', data=json.dumps({'query':q}).encode(), headers=MH, method='POST')
            with urllib.request.urlopen(req, timeout=30) as r: return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if a<retries-1: time.sleep(3); continue
            raise


def chunks(lst, n):
    for i in range(0, len(lst), n): yield lst[i:i+n]


def to_float(v):
    if v is None or v == '': return 0.0
    try: return float(str(v).replace(',','').replace('$','').strip())
    except: return 0.0


def normf(v):
    if v is None or v == '': return ''
    try: return f'{float(v):.2f}'
    except: return ''


def truncate(s, n=100):
    if not s: return ""
    s = str(s).replace("\n"," ").strip()
    return s[:n]


def extract_ref(text_or_name, texto_field=''):
    """Get PROP-XXX from texto field first, else from name/title text."""
    if texto_field and texto_field.upper().startswith('PROP-'):
        return texto_field.upper()
    if not text_or_name: return ''
    m = PROP_RE.search(text_or_name)
    return m.group(0).upper() if m else ''


# ---------------- STATE MANAGEMENT ----------------

def parse_ts(ts):
    """Parse ISO 8601 timestamp to datetime (handles Z and +00:00)."""
    if not ts:
        return None
    ts = ts.replace('Z', '+00:00')
    try:
        return datetime.fromisoformat(ts)
    except (ValueError, TypeError):
        return None


def load_state():
    """Load last run timestamp from state file."""
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state):
    """Save run state to file."""
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


def get_changed_refs(propuestas, since):
    """Return set of PROP-XXX refs whose Monday propuesta item was updated since `since`."""
    since_dt = parse_ts(since)
    if not since_dt:
        return set(p['ref'] for p in propuestas.values() if p['ref'])
    changed = set()
    for p in propuestas.values():
        item_dt = parse_ts(p.get('updated_at', ''))
        if item_dt and item_dt >= since_dt and p['ref']:
            changed.add(p['ref'])
    return changed


def get_changed_upsell_refs(upsells, since):
    """Return set of PROP-XXX refs whose Monday upsell item was updated since `since`."""
    since_dt = parse_ts(since)
    if not since_dt:
        return set(upsells.keys())
    changed = set()
    for ref, items in upsells.items():
        for info in items:
            item_dt = parse_ts(info.get('updated_at', ''))
            if item_dt and item_dt >= since_dt:
                changed.add(ref)
                break
    return changed


def get_changed_opp_ids(opps, since):
    """Return set of Monday opp IDs whose item was updated since `since`."""
    since_dt = parse_ts(since)
    if not since_dt:
        return set(opps.keys())
    changed = set()
    for oid, o in opps.items():
        item_dt = parse_ts(o.get('updated_at', ''))
        if item_dt and item_dt >= since_dt:
            changed.add(oid)
    return changed


# ---------------- DATA LOADERS ----------------

def load_monday_opps():
    """Return dict opp_id -> {status, propuestas, updated_at, name}"""
    print('  loading Monday Oportunidades...', flush=True)
    opps = {}
    cursor = None
    while True:
        cur = f', cursor: "{cursor}"' if cursor else ''
        d = monday(f"""query {{ boards(ids: [{OPORTUNIDADES_BOARD}]) {{ items_page(limit: 500{cur}) {{ cursor items {{
              id name updated_at column_values(ids: ["status","conectar_tableros0","link_to_propuestas"]) {{
                id text ... on BoardRelationValue {{ linked_items {{ id }} }} }} }} }} }} }}""")
        page = d.get('data',{}).get('boards',[{}])[0].get('items_page',{})
        for it in page.get('items',[]):
            cvs = {cv['id']:cv for cv in it.get('column_values',[])}
            plinks = set()
            for col in ('conectar_tableros0','link_to_propuestas'):
                for li in (cvs.get(col,{}).get('linked_items') or []): plinks.add(li['id'])
            opps[it['id']] = {
                'name': it['name'],
                'status': (cvs.get('status',{}).get('text') or '').strip(),
                'propuestas': list(plinks),
                'updated_at': it.get('updated_at', ''),
            }
        cursor = page.get('cursor')
        if not cursor: break
        time.sleep(0.4)
    return opps


def load_monday_propuestas():
    """Return dict prop_item_id -> {ref, status, monthly, annual, oneshot, accepted_at, name, updated_at}"""
    print('  loading Monday Propuestas...', flush=True)
    props = {}
    cursor = None
    while True:
        cur = f', cursor: "{cursor}"' if cursor else ''
        d = monday(f"""query {{ boards(ids: [{PROPUESTAS_BOARD}]) {{ items_page(limit: 500{cur}) {{ cursor items {{
              id name updated_at column_values(ids: ["texto","texto1","status","dup__of_final_value","numeric5","numeric","fecha5"]) {{ id text }}
            }} }} }} }}""")
        page = d.get('data',{}).get('boards',[{}])[0].get('items_page',{})
        for it in page.get('items',[]):
            cvs = {cv['id']:cv for cv in it.get('column_values',[])}
            texto = (cvs.get('texto',{}).get('text') or '').strip()
            ref = extract_ref(it.get('name',''), texto)
            if not ref: continue
            props[it['id']] = {
                'ref': ref,
                'name': (cvs.get('texto1',{}).get('text') or '').strip() or it.get('name',''),
                'status': (cvs.get('status',{}).get('text') or '').strip(),
                'monthly': to_float(cvs.get('dup__of_final_value',{}).get('text')),
                'annual':  to_float(cvs.get('numeric5',{}).get('text')),
                'oneshot': to_float(cvs.get('numeric',{}).get('text')),
                'accepted_at': (cvs.get('fecha5',{}).get('text') or '').strip()[:10],
                'updated_at': it.get('updated_at', ''),
            }
        cursor = page.get('cursor')
        if not cursor: break
        time.sleep(0.4)
    return props


def load_monday_upsells():
    """Return dict PROP-XXX -> [{monthly, annual, oneshot, accepted_at, status, updated_at}]"""
    print('  loading Monday Upselling...', flush=True)
    upsells = defaultdict(list)
    cursor = None
    while True:
        cur = f', cursor: "{cursor}"' if cursor else ''
        d = monday(f"""query {{ boards(ids: [{UPSELLING_BOARD}]) {{ items_page(limit: 500{cur}) {{ cursor items {{
              id name updated_at column_values(ids: ["texto5","status","dup__of_prop_value","numeric5","numeric","dup__of_fup_3"]) {{ id text }}
            }} }} }} }}""")
        page = d.get('data',{}).get('boards',[{}])[0].get('items_page',{})
        for it in page.get('items',[]):
            cvs = {cv['id']:cv for cv in it.get('column_values',[])}
            texto = (cvs.get('texto5',{}).get('text') or '').strip()
            ref = extract_ref(it.get('name',''), texto)
            if not ref: continue
            upsells[ref].append({
                'monthly': to_float(cvs.get('dup__of_prop_value',{}).get('text')),
                'annual':  to_float(cvs.get('numeric5',{}).get('text')),
                'oneshot': to_float(cvs.get('numeric',{}).get('text')),
                'accepted_at': (cvs.get('dup__of_fup_3',{}).get('text') or '').strip()[:10],
                'status': (cvs.get('status',{}).get('text') or '').strip(),
                'updated_at': it.get('updated_at', ''),
            })
        cursor = page.get('cursor')
        if not cursor: break
        time.sleep(0.4)
    return upsells


def load_hs_deals(pipeline, properties):
    """Return list of all deals in pipeline with given properties."""
    deals = []
    after = None
    while True:
        body = {'filterGroups':[{'filters':[{'propertyName':'pipeline','operator':'EQ','value':pipeline}]}],
                'properties':properties,'limit':100,
                'sorts':[{'propertyName':'createdate','direction':'DESCENDING'}]}
        if after: body['after']=after
        st, d = hs('POST','/crm/v3/objects/deals/search',body)
        deals.extend(d.get('results',[]))
        after = d.get('paging',{}).get('next',{}).get('after')
        if not after: break
        time.sleep(0.2)
    return deals


def load_hs_quotes():
    """Return dict PROP-XXX -> {quote_id, title, status_tag, locked}.
    Locked quotes cannot be updated via PATCH — they need to be flagged so
    Step 3 skips them."""
    print('  loading HS Quotes...', flush=True)
    quotes = {}
    after = None
    while True:
        body = {'limit':100,'properties':['hs_title','hs_locked'],'sorts':[{'propertyName':'hs_createdate','direction':'DESCENDING'}]}
        if after: body['after']=after
        st, d = hs('POST','/crm/v3/objects/quotes/search',body)
        for q in d.get('results',[]):
            title = q['properties'].get('hs_title','') or ''
            locked = (q['properties'].get('hs_locked') or '').lower() == 'true'
            m = PROP_RE.search(title)
            if m:
                ref = m.group(0).upper()
                stat_m = re.match(r'^\[([A-Z]+)\]', title)
                status = stat_m.group(1) if stat_m else ''
                quotes[ref] = {'qid': q['id'], 'title': title, 'status_tag': status, 'locked': locked}
        after = d.get('paging',{}).get('next',{}).get('after')
        if not after: break
        time.sleep(0.15)
    return quotes


# ---------------- SYNC STEPS ----------------

def step1_refresh_ignition_all_quote_ids(opps, props, dry_run=False):
    """Update ignition_all_quote_ids on Ventas deals (comma-joined PROP-XXX list).
    Source: Monday Oportunidad propuestas_linked → ref."""
    print('\n=== STEP 1: refresh ignition_all_quote_ids on Ventas ===', flush=True)
    deals = load_hs_deals(VENTAS_PIPELINE, ['monday_deal_id','ignition_all_quote_ids'])
    print(f'  Ventas deals: {len(deals)}')
    updates = []
    for deal in deals:
        p = deal.get('properties') or {}
        mdi = (p.get('monday_deal_id') or '').strip()
        if not mdi: continue
        opp = opps.get(mdi)
        if not opp: continue
        refs = set()
        for pid in opp['propuestas']:
            pr = props.get(pid)
            if pr and pr['ref']: refs.add(pr['ref'])
        if not refs: continue
        new_val = ','.join(sorted(refs, key=lambda x: int(x.split('-')[1]) if '-' in x else 0))
        cur_val = (p.get('ignition_all_quote_ids') or '').strip()
        if cur_val == new_val: continue
        updates.append({'id':deal['id'],'properties':{'ignition_all_quote_ids':new_val}})
    print(f'  Updates: {len(updates)}')
    if dry_run or not updates: return 0
    return _apply_batch_update('deals', updates)


def step2_create_missing_quotes(propuestas, ref_to_deals, existing_quotes, dry_run=False):
    """Create HS Quote objects for any PROP-XXX in deals but missing.
    Uses Monday Propuestas as source (no Ignition CSV needed)."""
    print('\n=== STEP 2: create missing quotes (source: Monday Propuestas) ===', flush=True)
    # Index propuestas by ref (prefer Accepted/Completed status if duplicated)
    ref_info = {}
    for p in propuestas.values():
        r = p['ref']
        if not r: continue
        if r not in ref_info:
            ref_info[r] = p
        else:
            old_s = ref_info[r]['status']
            if p['status'] in ('Accepted','Completed') and old_s not in ('Accepted','Completed'):
                ref_info[r] = p

    to_create = []
    for ref, deal_ids in ref_to_deals.items():
        if ref in existing_quotes: continue
        info = ref_info.get(ref, {})
        state = info.get('status','')
        tag = STATE_TAG.get(state, 'UNKNOWN')
        name = info.get('name','') or ref
        title = f"[{tag}] {ref} — {truncate(name, 100)}"
        exp = info.get('accepted_at') or (datetime.now()+timedelta(days=365)).strftime('%Y-%m-%d')
        to_create.append({'ref':ref,'deals':deal_ids,'props':{
            'hs_title':title,'hs_status':'DRAFT','hs_currency':'USD','hs_language':'en','hs_expiration_date':exp,
        }})
    print(f'  Quotes to create: {len(to_create)}')
    if dry_run or not to_create: return 0

    created_count = 0
    new_qids_by_ref = {}
    for batch in chunks(to_create, 100):
        title_to_ref = {x['props']['hs_title']: x['ref'] for x in batch}
        inputs = [{'properties':x['props']} for x in batch]
        st, d = hs('POST','/crm/v3/objects/quotes/batch/create',{'inputs':inputs})
        if st in (200,201,207):
            for res in d.get('results',[]):
                title = (res.get('properties') or {}).get('hs_title','')
                ref = title_to_ref.get(title)
                if ref: new_qids_by_ref[ref] = res['id']; created_count += 1
        time.sleep(0.3)
    # Associate to deals
    assoc_inputs = []
    for x in to_create:
        qid = new_qids_by_ref.get(x['ref'])
        if not qid: continue
        for did in x['deals']:
            assoc_inputs.append({'from':{'id':qid},'to':{'id':did},'type':'quote_to_deal'})
    assoc_ok = 0
    for batch in chunks(assoc_inputs, 100):
        st, d = hs('POST','/crm/v3/associations/quotes/deals/batch/create',{'inputs':batch})
        if st in (200,201,207): assoc_ok += len(d.get('results',[]))
        time.sleep(0.2)
    print(f'  Created: {created_count}, associations: {assoc_ok}')
    return created_count


def step3_update_quote_titles(propuestas, existing_quotes, dry_run=False, changed_refs=None):
    """Update quote titles when status tag in title != current Monday status.
    If changed_refs is provided, only process those refs (incremental mode)."""
    print('\n=== STEP 3: update quote titles where status changed ===', flush=True)
    # Build PROP-XXX → current Monday status from propuestas dict (already loaded)
    ref_to_status = {}
    for p in propuestas.values():
        ref, status = p['ref'], p['status']
        if not ref: continue
        # If multiple proposals share same ref, prefer Accepted/Completed
        if ref not in ref_to_status:
            ref_to_status[ref] = (status, p['name'])
        else:
            curr_s = ref_to_status[ref][0]
            if status in ('Accepted','Completed') and curr_s not in ('Accepted','Completed'):
                ref_to_status[ref] = (status, p['name'])

    updates = []
    skipped_locked = 0
    for ref, q in existing_quotes.items():
        if changed_refs is not None and ref not in changed_refs: continue
        if ref not in ref_to_status: continue
        new_status, new_name = ref_to_status[ref]
        new_tag = STATE_TAG.get(new_status, 'UNKNOWN')
        cur_title = q['title']
        new_title = f"[{new_tag}] {ref} — {truncate(new_name, 100)}"
        if cur_title == new_title: continue
        if q.get('locked'):
            skipped_locked += 1; continue
        updates.append({'id':q['qid'],'properties':{'hs_title':new_title}})
    if changed_refs is not None:
        print(f'  Checking {len(changed_refs)} changed refs → {len(updates)} updates (skipped {skipped_locked} locked)')
    else:
        print(f'  Quote titles to update: {len(updates)} (skipped {skipped_locked} locked)')
    if dry_run or not updates: return 0
    # Quote PATCH batch endpoint silently fails when one item is invalid (rejecting the whole batch).
    # Use singles — slower but reliable.
    ok = fail = 0
    for u in updates:
        st, _ = hs('PATCH', f"/crm/v3/objects/quotes/{u['id']}", {'properties': u['properties']})
        if st in (200, 201, 204): ok += 1
        else: fail += 1
        time.sleep(0.12)
    print(f'  -> ok={ok}, fail={fail}')
    return ok


def step4_sync_ventas_money(propuestas, opps, dry_run=False, changed_refs=None):
    """Sync money + closedate on Ventas deals from Monday Propuestas.
    If changed_refs is provided, only process deals referencing those refs (incremental).
    NOTE: still uses FULL propuestas data for accurate sum calculation."""
    print('\n=== STEP 4: sync Ventas money + closedate ===', flush=True)
    # Build ref -> [propuesta info] (only Accepted/Completed)
    ref_to_acc = defaultdict(list)
    for p in propuestas.values():
        if p['status'] in ('Accepted','Completed') and p['ref']:
            ref_to_acc[p['ref']].append(p)

    deals = load_hs_deals(VENTAS_PIPELINE, ['dealname','closedate','monthly_recurring_amount','annual_recurring_amount','one_shot_amount',
                                            'ignition_all_quote_ids','ignition_quote_id','ignition_proposal_ref','dealstage'])
    updates = []
    skipped_no_delta = 0
    for deal in deals:
        p = deal.get('properties') or {}
        all_text = ' '.join([p.get(f) or '' for f in ('ignition_all_quote_ids','ignition_quote_id','ignition_proposal_ref')])
        refs = set(m.upper() for m in PROP_RE.findall(all_text))
        if not refs: continue
        # Incremental: skip deals whose propuestas didn't change
        if changed_refs is not None and not refs & changed_refs:
            skipped_no_delta += 1; continue
        sum_m = sum_a = sum_o = 0.0; max_date = ''
        any_accepted = False
        for ref in refs:
            for info in ref_to_acc.get(ref, []):
                any_accepted = True
                sum_m += info['monthly']; sum_a += info['annual']; sum_o += info['oneshot']
                if info['accepted_at'] and info['accepted_at'] > max_date: max_date = info['accepted_at']
        if not any_accepted: continue

        cur_m = to_float(p.get('monthly_recurring_amount'))
        cur_a = to_float(p.get('annual_recurring_amount'))
        cur_o = to_float(p.get('one_shot_amount'))
        cur_cd = (p.get('closedate') or '')[:10]
        np = {}
        if normf(cur_m) != normf(sum_m): np['monthly_recurring_amount'] = normf(sum_m)
        if normf(cur_a) != normf(sum_a): np['annual_recurring_amount'] = normf(sum_a)
        if normf(cur_o) != normf(sum_o): np['one_shot_amount'] = normf(sum_o)
        if max_date and cur_cd != max_date: np['closedate'] = max_date
        if np: updates.append({'id':deal['id'],'properties':np})
    if changed_refs is not None:
        print(f'  Checking deals with {len(changed_refs)} changed refs (skipped {skipped_no_delta} unchanged) -> {len(updates)} updates')
    else:
        print(f'  Updates: {len(updates)}')
    if dry_run or not updates: return 0
    return _apply_batch_update('deals', updates)


def step5_sync_upsells_money(upsells, dry_run=False, changed_refs=None):
    """Sync money + closedate on Upsells deals from Monday Upselling.
    If changed_refs is provided, only process deals referencing those refs (incremental)."""
    print('\n=== STEP 5: sync Upsells money + closedate ===', flush=True)
    deals = load_hs_deals(UPSELLS_PIPELINE, ['dealname','closedate','monthly_recurring_amount','annual_recurring_amount','one_shot_amount',
                                              'monday_upsell_id_propuesta','ignition_quote_id','ignition_proposal_ref','dealstage'])
    updates = []
    skipped_no_delta = 0
    for deal in deals:
        p = deal.get('properties') or {}
        all_text = ' '.join([p.get(f) or '' for f in ('monday_upsell_id_propuesta','ignition_quote_id','ignition_proposal_ref')])
        refs = set(m.upper() for m in PROP_RE.findall(all_text))
        if not refs: continue
        if changed_refs is not None and not refs & changed_refs:
            skipped_no_delta += 1; continue
        sum_m = sum_a = sum_o = 0.0; max_date = ''
        any_accepted = False
        for ref in refs:
            for info in upsells.get(ref, []):
                if info['status'] != 'Accepted': continue
                any_accepted = True
                sum_m += info['monthly']; sum_a += info['annual']; sum_o += info['oneshot']
                if info['accepted_at'] and info['accepted_at'] > max_date: max_date = info['accepted_at']
        if not any_accepted: continue

        cur_m = to_float(p.get('monthly_recurring_amount'))
        cur_a = to_float(p.get('annual_recurring_amount'))
        cur_o = to_float(p.get('one_shot_amount'))
        cur_cd = (p.get('closedate') or '')[:10]
        np = {}
        if normf(cur_m) != normf(sum_m): np['monthly_recurring_amount'] = normf(sum_m)
        if normf(cur_a) != normf(sum_a): np['annual_recurring_amount'] = normf(sum_a)
        if normf(cur_o) != normf(sum_o): np['one_shot_amount'] = normf(sum_o)
        if max_date and cur_cd != max_date: np['closedate'] = max_date
        if np: updates.append({'id':deal['id'],'properties':np})
    if changed_refs is not None:
        print(f'  Checking deals with {len(changed_refs)} changed refs (skipped {skipped_no_delta} unchanged) -> {len(updates)} updates')
    else:
        print(f'  Updates: {len(updates)}')
    if dry_run or not updates: return 0
    return _apply_batch_update('deals', updates)


def step6_sync_dealstage(opps, dry_run=False, changed_opp_ids=None):
    """Sync dealstage on Ventas deals to match Monday Oportunidad status.
    If changed_opp_ids is provided, only process deals with those Monday IDs (incremental)."""
    print('\n=== STEP 6: sync Ventas dealstage from Monday Oportunidad ===', flush=True)
    deals = load_hs_deals(VENTAS_PIPELINE, ['dealname','dealstage','monday_deal_id'])
    updates = []
    skipped_no_delta = 0
    for deal in deals:
        p = deal.get('properties') or {}
        mdi = (p.get('monday_deal_id') or '').strip()
        if not mdi: continue
        if changed_opp_ids is not None and mdi not in changed_opp_ids:
            skipped_no_delta += 1; continue
        opp = opps.get(mdi)
        if not opp: continue
        target = STAGE_MAP.get(opp['status'])
        if not target: continue
        if p.get('dealstage') != target:
            updates.append({'id':deal['id'],'properties':{'dealstage':target}})
    if changed_opp_ids is not None:
        print(f'  Checking {len(changed_opp_ids)} changed opps (skipped {skipped_no_delta} unchanged) -> {len(updates)} updates')
    else:
        print(f'  Updates: {len(updates)}')
    if dry_run or not updates: return 0
    return _apply_batch_update('deals', updates)


# ---------------- HELPERS ----------------

def _apply_batch_update(object_type, updates):
    ok = fail = 0
    for batch in chunks(updates, 100):
        inputs = [{'id':u['id'],'properties':u['properties']} for u in batch]
        st, d = hs('POST',f'/crm/v3/objects/{object_type}/batch/update',{'inputs':inputs})
        if st in (200,201,207):
            errs = d.get('errors') or []
            ok += len(batch)-len(errs); fail += len(errs)
        else:
            fail += len(batch)
        time.sleep(0.2)
    print(f'  -> ok={ok}, fail={fail}')
    return ok


def load_ignition_csv():
    """Return PROP-XXX -> info dict"""
    print('  loading Ignition CSV...', flush=True)
    ign = {}
    try:
        with open(IGN_CSV) as f:
            for r in csv.DictReader(f):
                ref = (r.get('Proposal Reference') or '').strip().upper()
                if not ref: continue
                ign[ref] = {
                    'state': (r.get('Proposal State') or '').strip(),
                    'name': (r.get('Proposal Name') or '').strip(),
                    'client_id': (r.get('Client ID') or '').strip(),
                    'amount': (r.get('Minimum Value (Total)') or '').strip(),
                    'accepted_at': (r.get('Proposal Accepted At') or '').strip()[:10],
                    'created_at': (r.get('Proposal Created At') or '').strip()[:10],
                    'effective_start': (r.get('Effective Start Date') or '').strip()[:10],
                    'renewal': (r.get('Renewal Date') or '').strip()[:10],
                }
        print(f'    {len(ign)} proposals')
    except FileNotFoundError:
        print(f'    WARNING: Ignition CSV not found at {IGN_CSV} — Step 2 (create quotes) will use minimal info')
    return ign


def build_ref_to_deals():
    """For Step 2: build PROP-XXX -> [deal_ids] across Ventas + Upsells."""
    ref_to_deals = defaultdict(list)
    for pipe in (VENTAS_PIPELINE, UPSELLS_PIPELINE):
        deals = load_hs_deals(pipe, ['ignition_all_quote_ids','ignition_quote_id','ignition_proposal_ref','monday_upsell_id_propuesta'])
        for deal in deals:
            p = deal.get('properties') or {}
            text = ' '.join([p.get(f) or '' for f in ('ignition_all_quote_ids','ignition_quote_id','ignition_proposal_ref','monday_upsell_id_propuesta')])
            for ref in set(m.upper() for m in PROP_RE.findall(text)):
                ref_to_deals[ref].append(deal['id'])
    return ref_to_deals


# ---------------- MAIN ----------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip", default="", help="Comma-separated step numbers to skip (1-6)")
    ap.add_argument("--incremental", action="store_true",
                    help="Only process items changed since last run (steps 3-6)")
    ap.add_argument("--since-minutes", type=int, default=None,
                    help="Override state file: check items changed in last N minutes (for cloud/stateless use)")
    args = ap.parse_args()
    skip = set(int(s) for s in args.skip.split(',') if s.strip().isdigit())

    # Prevent overlapping runs (important for 10-min cron interval)
    lock_fp = open(LOCK_FILE, 'w')
    try:
        fcntl.flock(lock_fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (IOError, OSError):
        print('Another instance is already running — exiting.')
        return
    lock_fp.write(str(os.getpid()))
    lock_fp.flush()

    # Record run start time BEFORE loading data (items modified during load will
    # be caught in the next run since we use >= comparison)
    run_start = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    start = time.time()
    incremental = args.incremental

    # Load state for incremental mode
    since = None
    if args.since_minutes:
        # Stateless mode: fixed lookback window (used by cloud routines)
        incremental = True
        since = (datetime.now(timezone.utc) - timedelta(minutes=args.since_minutes)).strftime('%Y-%m-%dT%H:%M:%SZ')
    elif incremental:
        state = load_state()
        since = state.get('last_run_at')
        if not since:
            print('WARNING: No previous state found — running full sync instead\n', flush=True)
            incremental = False

    mode_label = "INCREMENTAL" if incremental else ("DRY-RUN" if args.dry_run else "FULL")
    print(f'=== SYNC START @ {datetime.now().isoformat()} ===')
    print(f'Mode: {mode_label}')
    if since:
        print(f'Since: {since}')
    print(flush=True)

    # Always load ALL Monday data — reads are cheap (~5 API calls per board),
    # and we need full data for accurate sum calculations even in incremental mode
    print('Loading source data from Monday...')
    opps = load_monday_opps()
    propuestas = load_monday_propuestas()
    upsells = load_monday_upsells()
    print(f'  {len(opps)} opps | {len(propuestas)} propuestas | {len(upsells)} upsell refs\n')

    # In incremental mode, compute which items changed since last run
    cr = cu = co = None  # changed propuesta refs, upsell refs, opp IDs
    if incremental:
        cr = get_changed_refs(propuestas, since)
        cu = get_changed_upsell_refs(upsells, since)
        co = get_changed_opp_ids(opps, since)
        total_changes = len(cr) + len(cu) + len(co)
        print(f'  Delta since {since}:')
        print(f'    Propuesta refs changed: {len(cr)}')
        print(f'    Upsell refs changed:    {len(cu)}')
        print(f'    Opp IDs changed:        {len(co)}')
        if total_changes == 0:
            save_state({'last_run_at': run_start, 'last_mode': 'incremental',
                        'last_result': 'no_changes', 'elapsed_s': round(time.time()-start, 1)})
            print(f'\n  No changes detected — done in {time.time()-start:.0f}s')
            return
        print()

    totals = {}
    existing_quotes = None

    # Steps 1-2: full-run only (structural changes are rare, run daily)
    if not incremental:
        if 1 not in skip:
            totals['step1'] = step1_refresh_ignition_all_quote_ids(opps, propuestas, args.dry_run)
        if 2 not in skip:
            ref_to_deals = build_ref_to_deals()
            existing_quotes = load_hs_quotes()
            totals['step2'] = step2_create_missing_quotes(propuestas, ref_to_deals, existing_quotes, args.dry_run)

    # Steps 3-6: run with optional delta filter
    if 3 not in skip and (not incremental or cr):
        if existing_quotes is None:
            existing_quotes = load_hs_quotes()
        totals['step3'] = step3_update_quote_titles(propuestas, existing_quotes, args.dry_run, cr)

    if 4 not in skip and (not incremental or cr):
        totals['step4'] = step4_sync_ventas_money(propuestas, opps, args.dry_run, cr)

    if 5 not in skip and (not incremental or cu):
        totals['step5'] = step5_sync_upsells_money(upsells, args.dry_run, cu)

    if 6 not in skip and (not incremental or co):
        totals['step6'] = step6_sync_dealstage(opps, args.dry_run, co)

    # Save state after successful run
    save_state({
        'last_run_at': run_start,
        'last_mode': 'incremental' if incremental else 'full',
        'last_totals': totals,
        'elapsed_s': round(time.time() - start, 1),
    })

    elapsed = time.time() - start
    print(f'\n=== SYNC DONE in {elapsed:.0f}s ({mode_label}) ===')
    for k, v in totals.items():
        print(f'  {k}: {v}')


if __name__ == '__main__':
    main()
