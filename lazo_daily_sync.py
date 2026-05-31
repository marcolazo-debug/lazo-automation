"""
LAZO — Daily Sync Monday → HubSpot
Corre una vez por dia. Lee los items modificados en Monday desde la ultima
ejecucion y los sincroniza en HubSpot.

Uso:
    python3 lazo_daily_sync.py

Estado guardado en: ~/lazo-automation/last_sync.txt
"""

import urllib.request, urllib.error, json, time, os, argparse
from datetime import datetime, timedelta, timezone

# ─── CONFIG ───────────────────────────────────────────────────────────────────
MO_TOKEN = os.environ.get("MONDAY_TOKEN", "")
HS_TOKEN = os.environ.get("HS_TOKEN", "")
if not MO_TOKEN or not HS_TOKEN:
    raise SystemExit("ERROR: MONDAY_TOKEN and HS_TOKEN environment variables are required")
MO_URL   = "https://api.monday.com/v2"
HS_BASE  = "https://api.hubapi.com"
MO_HEADERS = {"Authorization": MO_TOKEN, "Content-Type": "application/json", "API-Version": "2024-01"}
HS_HEADERS = {"Authorization": f"Bearer {HS_TOKEN}", "Content-Type": "application/json"}

SYNC_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".last_daily_sync.txt")

BOARDS = {
    "oportunidades": "5435285342",
    "account":       "4063839282",
    "partners":      "5115439899",
    "contact":       "4063839223",
    "propuestas":    "5435295373",
}

CLOSED_WON  = "3617654478"
PIPELINE_VENTAS   = "2250405610"
PIPELINE_PARTNERS = "2250383080"

STAGE_MAP = {
    # Pre-proposal / meeting stages
    "Meeting Scheduled":     "3617654468",
    "No Show":               "3617654469",  # Monday "No Show" → HS "Reschedule"
    "Reschedule":            "3617654469",
    "Cancelled":             "3617654470",
    "Disqualified":          "3617654471",
    # Proposal / negotiation stages
    "Create Quote":          "3617654472",  # Proposal Created
    "Proposal Send":         "3617654472",  # legacy typo of "Proposal Sent"
    # Note: HS stage "Proposal Sent" (3617654473) was deleted from pipeline; no Monday equivalent
    "Next Month":            "3617654474",  # Future Next Month
    "Future Next Month":     "3617654474",
    "Future":                "3617654475",  # Future Waitting Acceptance
    "Future Waitting Acceptance": "3617654475",
    "Future Awaiting":       "3617654475",
    "Awaiting Acceptance":   "3687871166",  # real Awaiting Acceptance stage (creado en mirror 18-may)
    "In Review":             "3617654476",
    "No Answer":             "3617654477",
    # Closed
    "Closed Won":            "3617654478",
    "Closed Won - Accepted": "3617654478",
    "Closed Lost":           "3617654479",
}

# ─── HELPERS ──────────────────────────────────────────────────────────────────
def mo_query(q, variables=None):
    body = json.dumps({"query": q, "variables": variables or {}}).encode()
    req = urllib.request.Request(MO_URL, data=body, headers=MO_HEADERS, method="POST")
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"    [MO ERROR] {e}")
        return None

def hs_get(path):
    req = urllib.request.Request(HS_BASE + path, headers=HS_HEADERS)
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())
    except: return None

def hs_post(path, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(HS_BASE + path, data=data, headers=HS_HEADERS, method="POST")
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())
    except: return None

def hs_patch(obj, oid, props):
    body = json.dumps({"properties": props}).encode()
    req = urllib.request.Request(f"{HS_BASE}/crm/v3/objects/{obj}/{oid}", data=body, headers=HS_HEADERS, method="PATCH")
    try:
        with urllib.request.urlopen(req) as r:
            r.read()
            return True
    except urllib.error.HTTPError as e:
        body_str = e.read().decode()
        print(f"    [HS PATCH ERROR] obj={obj} id={oid} code={e.code}")
        print(f"      props_sent={json.dumps(props)[:500]}")
        print(f"      response={body_str[:800]}")
        return False

def hs_create(obj, props):
    body = json.dumps({"properties": props}).encode()
    req = urllib.request.Request(f"{HS_BASE}/crm/v3/objects/{obj}", data=body, headers=HS_HEADERS, method="POST")
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body_str = e.read().decode()
        print(f"    [HS CREATE ERROR] obj={obj} code={e.code}")
        print(f"      props_sent={json.dumps(props)[:500]}")
        print(f"      response={body_str[:800]}")
        return None

def hs_associate(obj_from, id_from, obj_to, id_to, assoc_type):
    req = urllib.request.Request(
        f"{HS_BASE}/crm/v3/objects/{obj_from}/{id_from}/associations/{obj_to}/{id_to}/{assoc_type}",
        data=b"", headers=HS_HEADERS, method="PUT"
    )
    try:
        with urllib.request.urlopen(req) as r:
            r.read()
            return True
    except: return False

def find_in_hs(obj, prop, value):
    r = hs_post(f"/crm/v3/objects/{obj}/search", {
        "filterGroups": [{"filters": [{"propertyName": prop, "operator": "EQ", "value": str(value)}]}],
        "properties": ["hs_object_id"],
        "limit": 1
    })
    if r and r.get("results"):
        return r["results"][0]["id"]
    return None

def parse_num(val):
    try: return float(str(val).replace(",","").replace("$","").strip()) if val else 0
    except: return 0

def get_last_sync():
    if os.path.exists(SYNC_FILE):
        with open(SYNC_FILE) as f:
            return f.read().strip()
    # Default: 3 days ago
    return "2026-05-05T00:00:00Z"

def save_last_sync(ts):
    with open(SYNC_FILE, "w") as f:
        f.write(ts)

def get_modified_items(board_id, since):
    """Get all items modified since a given timestamp"""
    q = """
    query ($board_id: ID!, $cursor: String) {
      boards(ids: [$board_id]) {
        items_page(limit: 200, cursor: $cursor) {
          cursor
          items {
            id name updated_at created_at
            column_values { id text value
              ... on BoardRelationValue { linked_item_ids }
            }
          }
        }
      }
    }
    """
    all_items = []
    cursor = None
    while True:
        r = mo_query(q, {"board_id": board_id, "cursor": cursor})
        if not r or not r.get("data"): break
        page = r["data"]["boards"][0]["items_page"]
        for item in page["items"]:
            if item["updated_at"] >= since:
                all_items.append(item)
        cursor = page.get("cursor")
        if not cursor or not page["items"]: break
        time.sleep(0.3)
    return all_items

# ─── SYNC FUNCTIONS ───────────────────────────────────────────────────────────

def sync_accounts(items):
    """Account → Company"""
    created = updated = skipped = 0
    for item in items:
        cv = {c["id"]: c for c in item["column_values"]}
        monday_id = str(item["id"])

        props = {
            "name":               item["name"],
            "monday_account_id":  monday_id,
            "sub_status":         cv.get("status_17__1",{}).get("text","") or cv.get("status4",{}).get("text","") or "",
            "lead_channel":       cv.get("lead_source__1",{}).get("text","") or "",
            "account_type":       cv.get("type__1",{}).get("text","") or "",
            "country":            cv.get("country__1",{}).get("text","") or "",
            "industry":           cv.get("industry__1",{}).get("text","") or "",
        }
        props = {k: v for k, v in props.items() if v}

        hs_id = find_in_hs("companies", "monday_account_id", monday_id)
        time.sleep(0.15)

        if hs_id:
            if hs_patch("companies", hs_id, props):
                updated += 1
            else:
                skipped += 1
        else:
            result = hs_create("companies", props)
            time.sleep(0.15)
            if result:
                created += 1
            else:
                skipped += 1
        time.sleep(0.1)

    print(f"    Companies: {created} creadas | {updated} actualizadas | {skipped} errores")

def find_contact_from_dealname(dealname, company_name=""):
    """Parsea 'Company - Person - SaleType' y busca contact en HS por firstname+lastname.
       Devuelve contact_id si match con HIGH/MEDIUM confidence, sino None.
       HIGH: 1 candidato + email_domain matches company_name
       MEDIUM: múltiples candidatos pero 1 con email_domain match
       LOW (no devuelve): 1 candidato sin domain match, ambiguous, sin candidatos
    """
    import re as _re
    if not dealname: return None
    s = _re.sub(r'^\[Ventas?\]\s*', '', dealname.strip())
    # Strip trailing sale_type (NA NC / EA EC / etc.)
    m = _re.search(r'\s+-\s+(NA\s+NC|EA\s+EC|EA\s+NC|NA\s+EC)\s*$', s, _re.I)
    if m: s = s[:m.start()].strip()
    if " - " not in s: return None
    idx = s.rfind(" - ")
    person = s[idx+3:].strip()
    parts = [p for p in _re.split(r'\s+', person) if p]
    if not parts or len(parts) < 2: return None
    firstname = parts[0]
    lastname = " ".join(parts[1:]) if len(parts) <= 2 else parts[-1]
    # Search por lastname (más distintivo)
    body = {
        "filterGroups":[{"filters":[
            {"propertyName":"lastname","operator":"EQ","value":lastname}
        ]}],
        "properties":["firstname","lastname","email"],
        "limit": 10,
    }
    result = hs_post("/crm/v3/objects/contacts/search", body)
    if not result: return None
    candidates = result.get("results", [])
    if not candidates: return None
    # Filtrar también por firstname si coincide (case-insensitive)
    fn_lower = firstname.lower()
    matching = [c for c in candidates if (c["properties"].get("firstname","") or "").lower() == fn_lower]
    if matching:
        candidates = matching
    if len(candidates) == 1:
        cand = candidates[0]
        email = (cand["properties"].get("email") or "").lower()
        if _domain_matches_co(email, company_name):
            return cand["id"]
        # Single candidate sin domain match: solo aceptar si firstname coincide
        if matching:  # ya filtrado por firstname
            return cand["id"]
        return None
    # Multiple — buscar 1 con domain match
    domain_matches = [c for c in candidates
                      if _domain_matches_co((c["properties"].get("email") or "").lower(), company_name)]
    if len(domain_matches) == 1:
        return domain_matches[0]["id"]
    return None

def _domain_matches_co(email, company_name):
    import re as _re
    if not email or "@" not in email or not company_name: return False
    domain = email.split("@",1)[1].lower()
    co = _re.sub(r'[^a-z0-9]', '', company_name.lower())
    dom_base = _re.sub(r'\.(com|net|org|io|co|tech|app|ai|inc|llc)(\.[a-z]{2,3})?$', '', domain)
    dom_base = _re.sub(r'[^a-z0-9]', '', dom_base)
    if not dom_base or not co: return False
    if dom_base.startswith(co[:5]) or co.startswith(dom_base[:5]):
        return True
    common = sum(1 for c in dom_base if c in co)
    return common >= max(len(dom_base), 4) * 0.7


# ─── Helpers para el flow Monday Account → Contacts (mirror logic) ──────────────
def _get_monday_account_for_sync(account_id):
    """Pull Monday Account con linked contacts + metadata. Devuelve dict o None."""
    q = """query ($id: ID!) {
      items(ids:[$id]) {
        id name
        column_values(ids:["account_contact","label_mkm5w8kq","texto60","sector","texto22","status","label8"]) {
          id text
          ... on BoardRelationValue { linked_item_ids }
        }
      }
    }"""
    r = mo_query(q, {"id": account_id})
    if not r or not r.get("data") or not r["data"]["items"]: return None
    item = r["data"]["items"][0]
    cv = {c["id"]: c for c in item.get("column_values",[])}
    return {
        "id": item["id"], "name": item["name"],
        "linked_contact_ids": cv.get("account_contact",{}).get("linked_item_ids",[]) or [],
        "lead_channel": cv.get("label_mkm5w8kq",{}).get("text",""),
        "country": cv.get("texto60",{}).get("text",""),
        "industry": cv.get("sector",{}).get("text",""),
        "ignition_id": cv.get("texto22",{}).get("text",""),
        "status": cv.get("status",{}).get("text",""),
        "sub_status": cv.get("label8",{}).get("text",""),
    }


def _get_monday_contacts_for_sync(contact_ids):
    """Pull Monday Contacts por IDs. Devuelve dict id → datos."""
    if not contact_ids: return {}
    q = """query ($ids: [ID!]!) {
      items(ids:$ids) {
        id name
        column_values(ids:["texto","texto1","contact_email","contact_phone","t_tulo","texto5"]) {
          id text
        }
      }
    }"""
    r = mo_query(q, {"ids":[str(x) for x in contact_ids]})
    if not r or not r.get("data"): return {}
    out = {}
    for it in r["data"]["items"]:
        cv = {c["id"]: c for c in it.get("column_values",[])}
        out[it["id"]] = {
            "name": it["name"],
            "firstname": cv.get("texto",{}).get("text",""),
            "lastname": cv.get("texto1",{}).get("text",""),
            "email": (cv.get("contact_email",{}).get("text","") or "").lower().strip(),
            "phone": cv.get("contact_phone",{}).get("text",""),
            "jobtitle": cv.get("t_tulo",{}).get("text",""),
            "country": cv.get("texto5",{}).get("text",""),
        }
    return out


def _find_hs_contact_by_monday_or_email(monday_contact_id, email):
    """Find HS Contact: 1) por monday_contact_id, 2) fallback por email."""
    body = {"filterGroups":[{"filters":[{"propertyName":"monday_contact_id","operator":"EQ","value":str(monday_contact_id)}]}],
            "properties":["email"],"limit":1}
    r = hs_post("/crm/v3/objects/contacts/search", body)
    if r and r.get("results"): return r["results"][0]["id"]
    if email:
        body = {"filterGroups":[{"filters":[{"propertyName":"email","operator":"EQ","value":email}]}],
                "properties":["email"],"limit":1}
        r = hs_post("/crm/v3/objects/contacts/search", body)
        if r and r.get("results"): return r["results"][0]["id"]
    return None


def _create_hs_contact_from_monday(md_contact, monday_contact_id):
    """Crea HS Contact con datos de Monday. Solo si tiene email."""
    if not md_contact.get("email"): return None
    props = {"email": md_contact["email"], "monday_contact_id": str(monday_contact_id)}
    if md_contact.get("firstname"): props["firstname"] = md_contact["firstname"]
    if md_contact.get("lastname"): props["lastname"] = md_contact["lastname"]
    if md_contact.get("phone"): props["phone"] = md_contact["phone"]
    if md_contact.get("jobtitle"): props["jobtitle"] = md_contact["jobtitle"]
    if md_contact.get("country"): props["country"] = md_contact["country"]
    r = hs_create("contacts", props)
    if r: return r["id"]
    # race condition: ya existe por email → re-search
    return _find_hs_contact_by_monday_or_email(monday_contact_id, md_contact["email"])


def _create_hs_company_from_monday(md_account, monday_account_id):
    """Crea HS Company con datos de Monday Account."""
    props = {"name": md_account["name"], "monday_account_id": str(monday_account_id)}
    if md_account.get("country"): props["country"] = md_account["country"]
    if md_account.get("industry"): props["industry_custom"] = md_account["industry"]
    if md_account.get("ignition_id"): props["ignition_id"] = md_account["ignition_id"]
    if md_account.get("sub_status"): props["sub_status"] = md_account["sub_status"]
    if md_account.get("lead_channel"): props["lead_channel"] = md_account["lead_channel"]
    r = hs_create("companies", props)
    return r["id"] if r else None


def sync_oportunidades(items):
    """Oportunidades → Deals Ventas"""
    created = updated = skipped = 0
    for item in items:
        cv = {c["id"]: c for c in item["column_values"]}
        monday_id = str(item["id"])

        # Stage
        status_text = cv.get("status",{}).get("text","") or ""
        stage_id = STAGE_MAP.get(status_text, "3617654468")

        # Amounts from propuestas via conectar_tableros0
        linked_props = cv.get("conectar_tableros0",{}).get("linked_item_ids",[])

        props = {
            "dealname":       item["name"],
            "pipeline":       PIPELINE_VENTAS,
            "dealstage":      stage_id,
            "monday_deal_id": monday_id,
            "deal_create_date": cv.get("fecha2",{}).get("text","") or "",
            "meeting_scheduled_date":   cv.get("fecha2",{}).get("text","") or "",
            "closedate":      cv.get("fecha97",{}).get("text","") or "",
            "last_stage_update": cv.get("date_mkng3ekj",{}).get("text","") or "",
            "closed_lost_date": cv.get("date_mkqfhg0n",{}).get("text","") or "",
        }
        props = {k: v for k, v in props.items() if v}

        hs_id = find_in_hs("deals", "monday_deal_id", monday_id)
        time.sleep(0.15)

        if hs_id:
            if hs_patch("deals", hs_id, props):
                updated += 1
            else:
                skipped += 1
        else:
            result = hs_create("deals", props)
            time.sleep(0.15)
            if result:
                created += 1
                hs_id = result["id"]
            else:
                skipped += 1
                continue

        # ═══════════════════════════════════════════════════════════════════════════
        # ASOCIACIONES (estrategia primary: Monday Account → linked contacts → HS)
        # ═══════════════════════════════════════════════════════════════════════════
        # Source of truth: Monday Account tiene linked Contacts (column account_contact).
        # Para cada Monday Contact ID:
        #   - Find HS Contact por monday_contact_id (preferido) OR por email
        #   - Si NO existe en HS → CREATE con datos de Monday (firstname/lastname/email/phone/jobtitle)
        #   - Asociar Deal → Contact
        #   - Get Contact's HS Companies → asociar Deal → CADA Company (estrategia "asociar todas")
        # Fallback (si ningún contact tenía Company en HS):
        #   - find/create Company por monday_account_id desde Monday Account
        # Si todo lo de arriba falla → fallback final: parsear dealname (legacy)
        contacts_associated = 0
        companies_associated = 0
        company_name_for_match = ""
        if hs_id:
            linked_account = cv.get("conectar_tableros",{}).get("linked_item_ids",[])
            if linked_account:
                monday_account_id = str(linked_account[0])
                # Traverse Monday Account para obtener linked contacts + metadata
                monday_acc = _get_monday_account_for_sync(monday_account_id)
                time.sleep(0.2)
                company_name_for_match = (monday_acc or {}).get("name","") if monday_acc else ""

                deal_companies_to_assoc = set()  # set para dedup
                if monday_acc and monday_acc.get("linked_contact_ids"):
                    monday_contacts = _get_monday_contacts_for_sync(monday_acc["linked_contact_ids"])
                    time.sleep(0.2)
                    for mc_id, mc_data in monday_contacts.items():
                        # Find HS Contact
                        hs_ct = _find_hs_contact_by_monday_or_email(mc_id, mc_data.get("email",""))
                        time.sleep(0.1)
                        if not hs_ct and mc_data.get("email"):
                            # Crear contact con datos de Monday
                            hs_ct = _create_hs_contact_from_monday(mc_data, mc_id)
                            if hs_ct:
                                print(f"    [mirror] Created HS contact {hs_ct} from Monday {mc_id}")
                            time.sleep(0.1)
                        if not hs_ct: continue
                        # Asociar Deal → Contact
                        if hs_associate("deals", hs_id, "contacts", hs_ct, "deal_to_contact"):
                            contacts_associated += 1
                            time.sleep(0.07)
                        # Get Contact's companies → asociar todas al Deal
                        ct_cos = hs_get(f"/crm/v4/objects/contacts/{hs_ct}/associations/companies?limit=10")
                        if ct_cos:
                            for ass in ct_cos.get("results",[]):
                                deal_companies_to_assoc.add(str(ass["toObjectId"]))
                        time.sleep(0.1)

                # Asociar Deal → cada Company del contact
                for co_id in deal_companies_to_assoc:
                    if hs_associate("deals", hs_id, "companies", co_id, "deal_to_company"):
                        companies_associated += 1
                    time.sleep(0.07)

                # Fallback Company: si ningún contact tenía Company → find/create por monday_account_id
                if not deal_companies_to_assoc:
                    co_id = find_in_hs("companies", "monday_account_id", monday_account_id)
                    time.sleep(0.1)
                    if not co_id and monday_acc:
                        co_id = _create_hs_company_from_monday(monday_acc, monday_account_id)
                        if co_id:
                            print(f"    [mirror] Created HS company {co_id} from Monday Account {monday_account_id}")
                        time.sleep(0.1)
                    if co_id:
                        if hs_associate("deals", hs_id, "companies", co_id, "deal_to_company"):
                            companies_associated += 1
                            time.sleep(0.1)

            # ─── FALLBACK FINAL: si aún no hay contact, parsear dealname (legacy) ──────
            if contacts_associated == 0:
                dealname = item["name"] or ""
                ct_id = find_contact_from_dealname(dealname, company_name_for_match)
                if ct_id:
                    hs_associate("deals", hs_id, "contacts", ct_id, "deal_to_contact")
                    time.sleep(0.1)
                    print(f"    [legacy-fallback] dealname-based contact match: deal={hs_id} → contact={ct_id}")
        time.sleep(0.1)

    print(f"    Deals Ventas: {created} creados | {updated} actualizados | {skipped} errores")

def sync_contacts(items):
    """Contact → Contacts. Usa column IDs CORRECTOS del Monday Contact board:
       texto (firstname), texto1 (lastname), contact_email, contact_phone, t_tulo (jobtitle).
       Si email vacío → fallback firstname/lastname desde item.name → fallback skip create.
    """
    created = updated = skipped = no_email_skipped = 0
    for item in items:
        cv = {c["id"]: c for c in item["column_values"]}
        monday_id = str(item["id"])

        # Column IDs correctos (verificados contra board 4063839223)
        email = (cv.get("contact_email",{}).get("text","") or "").lower().strip()
        firstname = cv.get("texto",{}).get("text","").strip()
        lastname = cv.get("texto1",{}).get("text","").strip()
        phone = cv.get("contact_phone",{}).get("text","").strip()
        jobtitle = cv.get("t_tulo",{}).get("text","").strip()
        country = cv.get("texto5",{}).get("text","").strip()

        # Fallback firstname/lastname desde item.name si Monday no tiene los campos separados
        if not firstname and not lastname and item.get("name"):
            parts = item["name"].split(" ")
            firstname = parts[0] if parts else ""
            lastname = " ".join(parts[1:]) if len(parts) > 1 else ""

        props = {
            "firstname":          firstname,
            "lastname":           lastname,
            "email":              email,
            "phone":              phone,
            "jobtitle":           jobtitle,
            "country":            country,
            "monday_contact_id":  monday_id,
        }
        props = {k: v for k, v in props.items() if v}

        # Try find existing por monday_contact_id PRIMERO
        hs_id = find_in_hs("contacts", "monday_contact_id", monday_id)
        time.sleep(0.15)
        # Fallback: si no encontró por monday_contact_id pero tenemos email, find por email
        # (evita duplicar contacts que ya existían por otra fuente sin monday_id)
        if not hs_id and email:
            hs_id = find_in_hs("contacts", "email", email)
            time.sleep(0.15)

        # ⚠️ Si va a CREATE pero no tiene email → SKIP. No crear contacts sin email
        # (HubSpot dedupea por email, sin él inflamos la base con basura).
        if not hs_id and not email:
            no_email_skipped += 1
            continue

        if hs_id:
            if hs_patch("contacts", hs_id, props):
                updated += 1
            else:
                skipped += 1
        else:
            result = hs_create("contacts", props)
            time.sleep(0.15)
            if result:
                created += 1
            else:
                skipped += 1
        time.sleep(0.1)

    print(f"    Contacts: {created} creados | {updated} actualizados | {skipped} errores | {no_email_skipped} skip (sin email Monday)")

def sync_partners(items):
    """Partners → Deals Partners"""
    created = updated = skipped = 0
    for item in items:
        monday_id = str(item["id"])
        cv = {c["id"]: c for c in item["column_values"]}

        props = {
            "dealname":          f"Partner — {item['name']}",
            "pipeline":          PIPELINE_PARTNERS,
            "monday_partner_id": monday_id,
        }

        hs_id = find_in_hs("deals", "monday_partner_id", monday_id)
        time.sleep(0.15)

        if hs_id:
            if hs_patch("deals", hs_id, props):
                updated += 1
            else:
                skipped += 1
        else:
            result = hs_create("deals", props)
            time.sleep(0.15)
            if result:
                created += 1
            else:
                skipped += 1
        time.sleep(0.1)

    print(f"    Deals Partners: {created} creados | {updated} actualizados | {skipped} errores")

def sync_propuestas(items):
    """Propuestas → actualizar amounts en Deals"""
    updated = skipped = 0
    for item in items:
        cv = {c["id"]: c for c in item["column_values"]}

        # Get linked deal
        linked = cv.get("board_relation",{}).get("linked_item_ids",[]) or \
                 cv.get("conectar_tableros0",{}).get("linked_item_ids",[])
        if not linked: continue

        monday_deal_id = str(linked[0])
        mrr      = parse_num(cv.get("dup__of_final_value",{}).get("text"))
        arr      = parse_num(cv.get("numeric5",{}).get("text"))
        one_shot = parse_num(cv.get("numeric",{}).get("text"))
        total    = mrr + arr + one_shot
        if total == 0: continue

        fecha5      = cv.get("fecha5",{}).get("text","") or ""
        status_text = cv.get("status",{}).get("text","") or ""

        hs_deal_id = find_in_hs("deals", "monday_deal_id", monday_deal_id)
        time.sleep(0.15)
        if not hs_deal_id: continue

        props = {
            "monthly_recurring_amount": mrr,
            "annual_recurring_amount":  arr,
            "one_shot_amount":          one_shot,
            "amount":                   total,
        }
        if fecha5:
            props["closedate"] = fecha5[:10]

        if hs_patch("deals", hs_deal_id, props):
            # Move to Closed Won if accepted
            if "Accepted" in status_text or "accepted" in status_text.lower():
                hs_patch("deals", hs_deal_id, {"dealstage": CLOSED_WON})
            updated += 1
        else:
            skipped += 1
        time.sleep(0.1)

    print(f"    Propuestas (amounts): {updated} actualizadas | {skipped} errores")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since-minutes", type=int, default=None,
                    help="Override state file: check items changed in last N minutes (for cloud/stateless use)")
    args = ap.parse_args()

    if args.since_minutes:
        since = (datetime.now(timezone.utc) - timedelta(minutes=args.since_minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        since = get_last_sync()
    now   = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    print("=" * 65)
    print(f"LAZO — Daily Sync Monday → HubSpot")
    print(f"Desde: {since}")
    print(f"Hasta: {now}")
    print("=" * 65)

    # 1. Accounts → Companies
    print("\n[1/5] Sincronizando Accounts → Companies...")
    items = get_modified_items(BOARDS["account"], since)
    print(f"  {len(items)} items modificados")
    if items: sync_accounts(items)

    # 2. Oportunidades → Deals Ventas
    print("\n[2/5] Sincronizando Oportunidades → Deals Ventas...")
    items = get_modified_items(BOARDS["oportunidades"], since)
    print(f"  {len(items)} items modificados")
    if items: sync_oportunidades(items)

    # 3. Contacts
    print("\n[3/5] Sincronizando Contacts...")
    items = get_modified_items(BOARDS["contact"], since)
    print(f"  {len(items)} items modificados")
    if items: sync_contacts(items)

    # 4. Partners → Deals Partners
    print("\n[4/5] Sincronizando Partners...")
    items = get_modified_items(BOARDS["partners"], since)
    print(f"  {len(items)} items modificados")
    if items: sync_partners(items)

    # 5. Propuestas → Amounts en Deals
    print("\n[5/5] Sincronizando Propuestas → Amounts...")
    items = get_modified_items(BOARDS["propuestas"], since)
    print(f"  {len(items)} items modificados")
    if items: sync_propuestas(items)

    # Save last sync
    save_last_sync(now)
    print(f"\n{'='*65}")
    print(f"✓ Sync completado. Próxima ejecución desde: {now}")
    print(f"{'='*65}")

if __name__ == "__main__":
    main()
