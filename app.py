# -*- coding: utf-8 -*-
from __future__ import annotations

"""
SQY Flex — Planification Missions - SQY Flex (MB1 / MB5)

Objectif:
- Même script Python en local + Railway/Render (python app.py)
- Page web qui NE REFRESH QUE si les réservations MILLA changent
  (pas de refresh permanent qui empêche d'utiliser la dropdown)

Dépendances:
    pip install fastapi uvicorn openpyxl requests python-dotenv
"""

import os
import re
import difflib
import unicodedata
import html
import time
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Dict, List, Tuple, Optional, Any

import openpyxl
from openpyxl.utils import column_index_from_string
import requests

# .env (optionnel mais recommandé)
try:
    from dotenv import load_dotenv
    from pathlib import Path
    load_dotenv(dotenv_path=Path(__file__).with_name(".env"))
except Exception:
    pass


# ============================================================
# CONFIG — À MODIFIER ICI UNIQUEMENT
# ============================================================

CONFIG: Dict[str, Any] = {
    "service": {
        "default_start_time": "11:37",
        "regulation_minutes": 15,  # default = 15 min
        "mode": "BUS",
        "use_stop_incompressible": True,
        "stop_incompressible_seconds": 60,
        "gain_seconds_when_no_activity": False,
        "non_terminus_dwell_is_max": True,
        "add_pax_to_incompressible": True,
        "planning_ignore_pax_dwell": True,
        "add_pax_at_terminus": False,
    },
    "passengers": {
        "pickup_seconds_per_pax": 30,
        "dropoff_seconds_per_pax": 20,
        "pax_per_booking": 1,
    },
    "fleet": {
        "shuttles": {
            "MB5": {"start_direction": "retour", "start_time": "11:37", "tours": 3},
            "MB1": {"start_direction": "aller",  "start_time": "11:37", "tours": 3},
        }
    },
    "routes": {
        "aller": [
            "Gare Routière des Prés 1",
            "SQY Ouest 2",
            "Fulgence Bienvenüe 2",
            "Les Chênes 2",
            "Pas du Lac",
            "Vieil Etang 1",
            "Vélodrome 1",
            "Gare Routière Paul Delouvrier 1",
        ],
        "retour": [
            "Gare Routière Paul Delouvrier 1",
            "Vélodrome 2",
            "Vieil Etang 2",
            "Pas du Lac",
            "Les Quadrants 1",
            "Les Chênes 1",
            "Fulgence Bienvenüe 1",
            "SQY Ouest 1",
            "Gare Routière des Prés 1",
        ],
    },
    "excel": {
        "preferred_filename": "Temps De Trajet.xlsx",
        "sheet_name": "temps",
        "origin_col": "C",
        "origin_row_start": 4,
        "origin_row_end": 34,
        "dest_row": 3,
        "dest_col_start": "D",
        "dest_col_end": "AH",
    },
    "api": {
        "base_url": "https://milla-sqy.millaapp.fr/api/v1/reservation/book/all-users",
        "service_date": "today",  # "today" OU "YYYY-MM-DD"
        "page_size": 500,
        "shuttle_map": {"61": "MB1", "65": "MB5"},
        "bearer_env_candidates": ["MILLA_BEARER", "MILLA_BEARER_TOKEN", "MILLA_BEARER_TOKEN_2"],
        "debug_print_first_item": False,
        "debug_print_counts": True,
    },
    "web": {
        "title": "Plan de mission - SQY Flex",
        "default_shuttle_view": "ALL",  # ALL / MB1 / MB5
        "poll_state_ms": 1000,          # fréquence du check (client -> /state)
        "pause_refresh_ms_on_ui": 4000, # pause quand tu touches la dropdown
    },
}


# ============================================================
# MODELS
# ============================================================

@dataclass
class Booking:
    reservation_id: int
    shuttle: str
    origin: str
    destination: str
    t: datetime
    pax: int = 1

@dataclass
class StopPass:
    shuttle: str
    station: str
    direction: str
    arrival: datetime
    departure: datetime
    pickup: int = 0
    dropoff: int = 0
    is_terminus_start: bool = False
    is_terminus_end: bool = False

@dataclass
class Mission:
    hour: str
    depart: str
    destination: str
    objective: str
    sort_dt: datetime


# ============================================================
# NORMALISATION / RESOLUTION DES NOMS
# ============================================================

def normalize_key(s: str) -> str:
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace("’", "'")
    s = re.sub(r"[^a-z0-9\s'\-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def build_station_resolver(stations_official: List[str]) -> Dict[str, str]:
    return {normalize_key(st): st for st in stations_official}

def resolve_station(name: str, resolver: Dict[str, str]) -> str:
    k = normalize_key(name)
    if k in resolver:
        return resolver[k]
    close = difflib.get_close_matches(k, list(resolver.keys()), n=5, cutoff=0.6)
    suggestions = [resolver[c] for c in close]
    raise KeyError(f"Station inconnue: '{name}'. Suggestions: {suggestions}")

def resolve_route_list(route: List[str], resolver: Dict[str, str]) -> List[str]:
    return [resolve_station(s, resolver) for s in route]


# ============================================================
# DATES / TIME
# ============================================================

def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

def get_service_date() -> date:
    cfg = str(CONFIG["api"].get("service_date", "today")).strip().lower()
    if cfg == "today":
        return date.today()
    return parse_date(cfg)

def parse_hhmm(s: str) -> Tuple[int, int]:
    hh, mm = s.strip().split(":")
    return int(hh), int(mm)

def combine(d: date, hhmm: str) -> datetime:
    hh, mm = parse_hhmm(hhmm)
    return datetime(d.year, d.month, d.day, hh, mm, 0)

def fmt_hhmm(dt: datetime) -> str:
    return dt.strftime("%H:%M:%S")


# ============================================================
# EXCEL (matrice)
# ============================================================

def find_excel_file(preferred: str) -> str:
    if os.path.exists(preferred):
        return preferred

    def ok(n: str) -> bool:
        low = n.lower()
        return (low.endswith(".xlsx") or low.endswith(".xlsm")) and ("temps" in low and "trajet" in low)

    cands = [f for f in os.listdir(".") if ok(f)]
    if not cands:
        raise FileNotFoundError(
            f"Fichier introuvable: '{preferred}' (ou un .xlsx/.xlsm contenant 'temps' et 'trajet')."
        )
    return sorted(cands)[0]

def load_travel_matrix_fixed(cfg_excel: dict) -> Tuple[Dict[str, Dict[str, float]], List[str]]:
    path = find_excel_file(cfg_excel["preferred_filename"])
    wb = openpyxl.load_workbook(path, data_only=True)

    sheet_wanted = str(cfg_excel.get("sheet_name", "")).strip()
    available = wb.sheetnames

    def _pick_sheet() -> str:
        if sheet_wanted and sheet_wanted in available:
            return sheet_wanted

        if sheet_wanted:
            def key(x: str) -> str:
                x = unicodedata.normalize("NFKD", x)
                x = "".join(ch for ch in x if not unicodedata.combining(ch))
                x = x.lower().strip()
                x = re.sub(r"\s+", " ", x)
                return x

            wanted_key = key(sheet_wanted)
            for s in available:
                if key(s) == wanted_key:
                    return s

            close = difflib.get_close_matches(sheet_wanted, available, n=1, cutoff=0.6)
            if close:
                return close[0]

        return available[0]

    sheet_selected = _pick_sheet()
    if sheet_wanted and sheet_selected != sheet_wanted:
        print(f"[INFO] Onglet demandé='{sheet_wanted}' introuvable. Onglet utilisé='{sheet_selected}'. Onglets dispo={available}")

    ws = wb[sheet_selected]

    origin_col = cfg_excel["origin_col"]
    r0 = int(cfg_excel["origin_row_start"])
    r1 = int(cfg_excel["origin_row_end"])
    dest_row = int(cfg_excel["dest_row"])
    c0 = column_index_from_string(cfg_excel["dest_col_start"])
    c1 = column_index_from_string(cfg_excel["dest_col_end"])

    dest_headers: List[Tuple[int, str]] = []
    for col in range(c0, c1 + 1):
        v = ws.cell(row=dest_row, column=col).value
        if v is None or str(v).strip() == "":
            continue
        dest_headers.append((col, str(v).strip()))
    if not dest_headers:
        raise ValueError("Aucune destination trouvée en D3:AH3.")

    matrix: Dict[str, Dict[str, float]] = {}
    origins: List[str] = []
    for r in range(r0, r1 + 1):
        o = ws[f"{origin_col}{r}"].value
        if o is None or str(o).strip() == "":
            continue
        origin = str(o).strip()
        origins.append(origin)
        matrix.setdefault(origin, {})
        for col_idx, dest in dest_headers:
            v = ws.cell(row=r, column=col_idx).value
            if v is None or v == "":
                continue
            matrix[origin][dest] = float(v)

    return matrix, origins

def travel_minutes(matrix: Dict[str, Dict[str, float]], a: str, b: str) -> float:
    try:
        return float(matrix[a][b])
    except KeyError:
        return 0.0


# ============================================================
# Pax / dwell rules
# ============================================================

def pax_seconds(pick: int, drop: int) -> int:
    return (
        int(pick) * int(CONFIG["passengers"]["pickup_seconds_per_pax"])
        + int(drop) * int(CONFIG["passengers"]["dropoff_seconds_per_pax"])
    )

def dwell_seconds(p: StopPass) -> int:
    # Planning dwell (arrêts) — ETA = A->C direct (sans pax)
    reg = int(CONFIG["service"]["regulation_minutes"]) * 60

    use_inc = bool(CONFIG["service"].get("use_stop_incompressible", True))
    inc = int(CONFIG["service"].get("stop_incompressible_seconds", 60)) if use_inc else 0

    # Temps pax (montées + descentes)
    px = pax_seconds(p.pickup, p.dropoff)

    # Flag: on ignore l'impact des pax UNIQUEMENT pour le shift de la tournée suivante
    ignore_pax_for_next_shift = bool(CONFIG["service"].get("planning_ignore_pax_dwell", False))

    # -----------------
    # Terminus de FIN
    # -----------------
    # La régulation décale la tournée suivante.
    # Si planning_ignore_pax_dwell=True, on N'AJOUTE PAS les pax ici (mais on garde la régulation).
    # Sinon, on peut ajouter les pax si explicitement demandé.
    if p.is_terminus_end:
        if bool(CONFIG["service"].get("add_pax_at_terminus", False)) and (not ignore_pax_for_next_shift):
            return reg + px
        return reg

    # -----------------
    # Terminus de DÉPART
    # -----------------
    if p.is_terminus_start:
        return 0

    # -----------------
    # Arrêts intermédiaires
    # -----------------
    if bool(CONFIG["service"].get("gain_seconds_when_no_activity", False)) and p.pickup == 0 and p.dropoff == 0:
        return 0

    if not use_inc:
        return px if bool(CONFIG["service"].get("add_pax_to_incompressible", False)) else 0

    # Ici, add_pax_to_incompressible DOIT fonctionner même si planning_ignore_pax_dwell=True
    if bool(CONFIG["service"].get("add_pax_to_incompressible", False)):
        return inc + px

    # Comportement historique
    if bool(CONFIG["service"].get("non_terminus_dwell_is_max", True)):
        return max(inc, px)

    return inc




# ============================================================
# API FETCH + SIGNATURE DES RESERVATIONS
# ============================================================

def _is_cancelled_status(raw: Any) -> bool:
    if raw is None:
        return False
    s = str(raw).strip().lower()
    if not s:
        return False
    cancelled = {
        "annulé", "annule",
        "cancelled", "canceled", "cancel",
        "cancelled_by_user", "canceled_by_user",
        "cancellation", "cancelation",
        "canceled_by_admin", "cancelled_by_admin",
    }
    return s in cancelled

def _parse_iso(dt_raw: Any) -> Optional[datetime]:
    if dt_raw is None:
        return None
    s = str(dt_raw).strip()
    if not s:
        return None
    s = s.replace("Z", "")
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        m = re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", s)
        if m:
            return datetime.fromisoformat(m.group(0))
    return None

def _extract_items(payload: Any) -> Tuple[List[dict], Optional[int]]:
    if isinstance(payload, dict) and "data" in payload and isinstance(payload["data"], dict):
        payload = payload["data"]

    if isinstance(payload, dict):
        content = payload.get("content")
        total_pages = payload.get("totalPages")
        if isinstance(content, list):
            return [x for x in content if isinstance(x, dict)], (int(total_pages) if isinstance(total_pages, int) else None)

    return [], None

def _get_token() -> str:
    for k in CONFIG["api"].get("bearer_env_candidates", []):
        v = os.environ.get(k, "").strip()
        if v:
            return v
    return ""

def _stable_reservation_signature(items_raw: List[dict]) -> str:
    """
    Signature stable:
    - On prend des champs "structurants" (id, time, status, depart, arrive, shuttleId)
    - On trie par (id, time, depart, arrive) => stable
    - On hash (sha1) => string courte
    """
    rows: List[Tuple[str, str, str, str, str, str]] = []
    for it in items_raw:
        rid = str(it.get("reservationId") or it.get("id") or "")
        t = str(it.get("reservationTime") or it.get("pickupTime") or it.get("requestedPickupTime") or "")
        st = str(it.get("status") or it.get("reservationStatus") or it.get("bookingStatus") or it.get("state") or "")
        dep = str(it.get("departStationName") or it.get("departureStationName") or "")
        arr = str(it.get("arriveStationName") or it.get("arrivalStationName") or "")
        sh = str(it.get("shuttleId") or it.get("vehicleId") or it.get("navetteId") or "")
        if not rid and not dep and not arr and not t:
            continue
        rows.append((rid, t, st, dep, arr, sh))

    rows.sort()
    blob = "\n".join("|".join(r) for r in rows).encode("utf-8", errors="ignore")
    return hashlib.sha1(blob).hexdigest()

def fetch_bookings_via_api(resolver: Dict[str, str], service_day: date) -> Tuple[List[Booking], str]:
    """
    Retourne:
      - bookings normalisés (pour calcul missions)
      - signature des réservations (pour savoir si ça a changé)
    """
    aconf = CONFIG["api"]
    token = _get_token()
    if not token:
        raise RuntimeError(
            "Bearer manquant. Mets ton token dans .env, ex:\n"
            "MILLA_BEARER=xxxxx\n"
            "(ou MILLA_BEARER_TOKEN=xxxxx)\n"
        )

    base_url = str(aconf["base_url"]).strip()
    page_size = int(aconf.get("page_size", 500))
    shuttle_map = aconf.get("shuttle_map", {})
    pax_per = int(CONFIG["passengers"].get("pax_per_booking", 1))

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    start = service_day.isoformat()
    end = service_day.isoformat()

    out: List[Booking] = []
    page = 0
    printed_first = False

    raw_all: List[dict] = []

    while True:
        params = {"page": page, "size": page_size, "start": start, "end": end}
        r = requests.get(base_url, headers=headers, params=params, timeout=30)
        if r.status_code == 401:
            raise RuntimeError("401 Unauthorized: token invalide/expiré (regenère un Bearer).")
        r.raise_for_status()

        data = r.json()
        items, total_pages = _extract_items(data)

        if aconf.get("debug_print_first_item", False) and (not printed_first) and items:
            print("[DEBUG] 1er item (snippet):", {k: items[0].get(k) for k in [
                "reservationId", "reservationTime", "status", "shuttleId", "routeId", "departStationName", "arriveStationName"
            ] if k in items[0]})
            printed_first = True

        if not items:
            break

        raw_all.extend(items)

        for it in items:
            status = it.get("status") or it.get("reservationStatus") or it.get("bookingStatus") or it.get("state")
            if _is_cancelled_status(status):
                continue

            dep = it.get("departStationName") or it.get("departureStationName")
            arr = it.get("arriveStationName") or it.get("arrivalStationName")
            if not dep or not arr:
                continue

            resa_dt = _parse_iso(it.get("reservationTime") or it.get("pickupTime") or it.get("requestedPickupTime"))
            if resa_dt is None:
                continue

            rid = int(it.get("reservationId") or 0)

            shuttle_id = it.get("shuttleId") or it.get("navetteId") or it.get("vehicleId")
            shuttle_raw = str(shuttle_id).strip() if shuttle_id is not None else ""
            shuttle = shuttle_map.get(shuttle_raw, shuttle_raw)

            try:
                origin = resolve_station(str(dep), resolver)
                destination = resolve_station(str(arr), resolver)
            except KeyError:
                continue

            out.append(Booking(
                reservation_id=rid,
                shuttle=shuttle,
                origin=origin,
                destination=destination,
                t=resa_dt,
                pax=pax_per,
            ))

        page += 1
        if total_pages is not None and page >= total_pages:
            break
        if total_pages is None and len(items) < page_size:
            break

    sig = _stable_reservation_signature(raw_all)

    if aconf.get("debug_print_counts", True):
        by: Dict[str, int] = {}
        for b in out:
            by[b.shuttle] = by.get(b.shuttle, 0) + 1
        print(f"[DEBUG] Bookings parsés total={len(out)} | par navette={by} | sig={sig[:10]}...")

    return out, sig


# ============================================================
# BUILD TIMELINE (BUS) — squelette + assignation résa + rebuild
# ============================================================

def build_shuttle_timeline_bus(shuttle: str, day: date, routes: Dict[str, List[str]], matrix: Dict[str, Dict[str, float]]) -> List[StopPass]:
    sconf = CONFIG["fleet"]["shuttles"][shuttle]
    tours = int(sconf.get("tours", 1))
    start_dir = str(sconf.get("start_direction", "aller"))
    start_time = str(sconf.get("start_time", CONFIG["service"]["default_start_time"]))

    t0 = combine(day, start_time)

    passes: List[StopPass] = []
    cur_dir = start_dir
    cur_t = t0

    for _ in range(tours):
        route = routes[cur_dir]
        last_idx = len(route) - 1

        for idx, station in enumerate(route):
            is_start = (idx == 0)
            is_end = (idx == last_idx)

            p = StopPass(
                shuttle=shuttle,
                station=station,
                direction=cur_dir,
                arrival=cur_t,
                departure=cur_t,
                pickup=0,
                dropoff=0,
                is_terminus_start=is_start,
                is_terminus_end=is_end,
            )

            if is_end:
                base_dwell = int(CONFIG["service"]["regulation_minutes"]) * 60
            elif is_start:
                base_dwell = 0
            else:
                base_dwell = int(CONFIG["service"]["stop_incompressible_seconds"]) if CONFIG["service"].get("use_stop_incompressible", True) else 0

            p.departure = p.arrival + timedelta(seconds=base_dwell)
            passes.append(p)

            if idx < last_idx:
                nxt = route[idx + 1]
                tr_min = travel_minutes(matrix, station, nxt)
                cur_t = p.departure + timedelta(minutes=tr_min)
            else:
                cur_t = p.departure

        cur_dir = "retour" if cur_dir == "aller" else "aller"

    return passes

def assign_reservations_to_timeline_bus(passes: List[StopPass], resas: List[Booking]) -> List[StopPass]:
    if not passes:
        return passes

    by_station: Dict[str, List[int]] = {}
    for i, p in enumerate(passes):
        by_station.setdefault(p.station, []).append(i)

    def find_best_pass_index(station: str, target_time: datetime) -> Optional[int]:
        idxs = by_station.get(station, [])
        if not idxs:
            return None
        best = None
        best_abs = None
        for i in idxs:
            diff = abs((passes[i].arrival - target_time).total_seconds())
            if best_abs is None or diff < best_abs:
                best_abs = diff
                best = i
        if best_abs is not None and best_abs <= 3600:
            return best
        return None

    for r in resas:
        dep_i = find_best_pass_index(r.origin, r.t)
        if dep_i is None:
            continue

        arr_i = None
        for j in range(dep_i + 1, len(passes)):
            if passes[j].station == r.destination:
                arr_i = j
                break
        if arr_i is None:
            continue

        passes[dep_i].pickup += r.pax
        passes[arr_i].dropoff += r.pax

    return passes

def rebuild_with_matrix_bus(passes: List[StopPass], matrix: Dict[str, Dict[str, float]], baseline: Optional[List[StopPass]] = None) -> List[StopPass]:
    if not passes:
        return passes

    rebuilt: List[StopPass] = []
    first0 = passes[0]
    first = StopPass(**{**first0.__dict__})
    first.departure = first.arrival + timedelta(seconds=dwell_seconds(first))
    rebuilt.append(first)

    for i in range(1, len(passes)):
        prev = rebuilt[i - 1]
        cur0 = passes[i]

        tm_min = travel_minutes(matrix, prev.station, cur0.station)
        if tm_min > 0:
            travel_sec = int(round(tm_min * 60))
        else:
            travel_sec = max(0, int((cur0.arrival - passes[i - 1].departure).total_seconds()))

        cur = StopPass(**{**cur0.__dict__})
        cur.arrival = prev.departure + timedelta(seconds=travel_sec)
        # dwell planifié (incluant pax selon paramètres)
        cur.departure = cur.arrival + timedelta(seconds=dwell_seconds(cur))

        # Si planning_ignore_pax_dwell=True, on garde les pax dans la tournée (arrêts intermédiaires),
        # mais on veut que l'heure de départ de la tournée suivante ne soit pas impactée par les pax.
        # On force donc le départ au terminus de FIN à suivre le planning "sans pax" (baseline),
        # ce qui revient à réduire la régulation disponible si la tournée a pris du retard.
        if cur.is_terminus_end and baseline is not None and bool(CONFIG["service"].get("planning_ignore_pax_dwell", False)):
            try:
                planned_dep = baseline[i].departure
                # On ne peut pas partir avant d'être arrivé : clamp sur arrival
                if planned_dep > cur.departure:
                    cur.departure = planned_dep
                elif planned_dep < cur.arrival:
                    cur.departure = cur.arrival
                else:
                    cur.departure = planned_dep
            except Exception:
                pass

        rebuilt.append(cur)

    return rebuilt


# ============================================================
# MISSIONS — logique old app (avec repositionnement, ETA)
# ============================================================

def objective_with_eta(pickup: int, dropoff: int, eta_hhmm: str) -> str:
    parts = []
    if pickup > 0:
        parts.append(f"↑ Pickup : {pickup}")
    if dropoff > 0:
        parts.append(f"↓ Drop Off : {dropoff}")
    parts.append(f"ETA : {eta_hhmm}")
    return " | ".join(parts)

def eta_for_leg(matrix: Dict[str, Dict[str, float]], frm: StopPass, dst: StopPass) -> str:
    direct_min = travel_minutes(matrix, frm.station, dst.station)
    # ETA demandé: uniquement temps de trajet direct (A -> C), sans temps montée/descente
    eta_dt = frm.departure + timedelta(minutes=direct_min)
    return fmt_hhmm(eta_dt)

def build_missions_old_style(passes: List[StopPass], matrix: Dict[str, Dict[str, float]]) -> List[Mission]:
    if not passes:
        return []

    def is_useful(p: StopPass) -> bool:
        return (p.pickup > 0) or (p.dropoff > 0)

    missions: List[Mission] = []
    n = len(passes)
    seg_start = 0

    for seg_end in range(n):
        if not passes[seg_end].is_terminus_end:
            continue

        nodes = [seg_start]
        for i in range(seg_start, seg_end + 1):
            if i != seg_start and is_useful(passes[i]):
                nodes.append(i)

        if seg_end not in nodes:
            nodes.append(seg_end)

        nodes = sorted(set(nodes))

        for a, b in zip(nodes, nodes[1:]):
            frm = passes[a]
            dst = passes[b]
            eta_hhmm = eta_for_leg(matrix, frm, dst)

            if dst.is_terminus_end and (not is_useful(dst)):
                obj = f"→ Repositionnement | ETA : {eta_hhmm}"
            else:
                obj = objective_with_eta(dst.pickup, dst.dropoff, eta_hhmm)

            missions.append(Mission(
                hour=fmt_hhmm(frm.departure),
                depart=frm.station,
                destination=dst.station,
                objective=obj,
                sort_dt=frm.departure,
            ))

        seg_start = seg_end + 1

    missions.sort(key=lambda m: m.sort_dt)
    return missions


# ============================================================
# CALCUL COMPLET + CACHES
# ============================================================

_CACHE: Dict[str, Any] = {"excel_mtime": None, "matrix": None, "stations": None, "resolver": None, "routes_resolved": None}

def _get_matrix_and_resolver_and_routes() -> Tuple[Dict[str, Dict[str, float]], Dict[str, str], Dict[str, List[str]]]:
    cfg_excel = CONFIG["excel"]
    excel_path = find_excel_file(cfg_excel["preferred_filename"])
    mtime = os.path.getmtime(excel_path)

    if _CACHE["excel_mtime"] != mtime or _CACHE["matrix"] is None:
        matrix, stations_official = load_travel_matrix_fixed(cfg_excel)
        resolver = build_station_resolver(stations_official)
        routes_resolved = {
            "aller": resolve_route_list(CONFIG["routes"]["aller"], resolver),
            "retour": resolve_route_list(CONFIG["routes"]["retour"], resolver),
        }
        _CACHE.update({
            "excel_mtime": mtime,
            "matrix": matrix,
            "stations": stations_official,
            "resolver": resolver,
            "routes_resolved": routes_resolved,
        })

    return _CACHE["matrix"], _CACHE["resolver"], _CACHE["routes_resolved"]

def compute_missions_by_shuttle(service_day: date) -> Tuple[Dict[str, List[Mission]], Dict[str, int], str]:
    matrix, resolver, routes_resolved = _get_matrix_and_resolver_and_routes()

    all_bookings, sig = fetch_bookings_via_api(resolver, service_day)

    by_shuttle: Dict[str, List[Booking]] = {}
    for b in all_bookings:
        if b.shuttle in CONFIG["fleet"]["shuttles"]:
            by_shuttle.setdefault(b.shuttle, []).append(b)

    reservation_counts: Dict[str, int] = {
        shuttle: len(by_shuttle.get(shuttle, []))
        for shuttle in CONFIG["fleet"]["shuttles"].keys()
    }

    missions_by: Dict[str, List[Mission]] = {}
    mode = str(CONFIG["service"].get("mode", "BUS")).upper().strip()

    for shuttle in CONFIG["fleet"]["shuttles"].keys():
        resas = by_shuttle.get(shuttle, [])

        bus_passes = build_shuttle_timeline_bus(shuttle, service_day, routes_resolved, matrix)
        bus_passes = assign_reservations_to_timeline_bus(bus_passes, resas)

        # Baseline "sans pax" pour figer l'heure de départ de la tournée suivante (shift)
        baseline_passes = None
        if bool(CONFIG["service"].get("planning_ignore_pax_dwell", False)):
            no_pax = [StopPass(**{**p.__dict__, "pickup": 0, "dropoff": 0}) for p in bus_passes]
            baseline_passes = rebuild_with_matrix_bus(no_pax, matrix, baseline=None)

        bus_passes = rebuild_with_matrix_bus(bus_passes, matrix, baseline=baseline_passes)

        passes_for_missions = bus_passes
        if mode == "TAD":
            passes_for_missions = bus_passes

        missions_by[shuttle] = build_missions_old_style(passes_for_missions, matrix)

    return missions_by, reservation_counts, sig


# Cache "résultat complet" pour protéger l'API (le client poll /state toutes les 1s)
_STATE_CACHE: Dict[str, Any] = {
    "day": None,
    "ts": 0.0,
    "missions_by": None,
    "reservation_counts": None,
    "sig": None,
}

def compute_state_cached(service_day: date, ttl_seconds: float = 1.0) -> Tuple[Dict[str, List[Mission]], Dict[str, int], str]:
    now = time.time()
    if (
        _STATE_CACHE["day"] == service_day
        and _STATE_CACHE["missions_by"] is not None
        and _STATE_CACHE["sig"] is not None
        and (now - float(_STATE_CACHE["ts"])) < float(ttl_seconds)
    ):
        return _STATE_CACHE["missions_by"], _STATE_CACHE["reservation_counts"], _STATE_CACHE["sig"]

    missions_by, reservation_counts, sig = compute_missions_by_shuttle(service_day)
    _STATE_CACHE.update({
        "day": service_day,
        "ts": now,
        "missions_by": missions_by,
        "reservation_counts": reservation_counts,
        "sig": sig,
    })
    return missions_by, reservation_counts, sig


# ============================================================
# RENDERING WEB (HTML)
# ============================================================

PAGE_STYLE = """
<style>
:root{
  --bg:#f6f7fb;
  --card:#ffffff;
  --text:#111827;
  --muted:#6b7280;
  --border:#e5e7eb;
  --header:#0f172a;
  --headerText:#ffffff;
  --shadow:0 6px 20px rgba(15,23,42,.08);
  --radius:14px;
}
*{box-sizing:border-box;}
body{
  margin:0;
  font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, "Apple Color Emoji","Segoe UI Emoji";
  background:var(--bg);
  color:var(--text);
}
.container{
  max-width:1200px;
  margin:28px auto;
  padding:0 18px 40px;
}
.topbar{
  display:flex;
  gap:14px;
  flex-wrap:wrap;
  align-items:center;
  justify-content:space-between;
  margin-bottom:18px;
}
.title{
  display:flex;
  flex-direction:column;
  gap:4px;
}
.title h1{
  margin:0;
  font-size:22px;
  font-weight:800;
  letter-spacing:.2px;
}
.subtitle{
  color:var(--muted);
  font-size:13px;
}
.controls{
  display:flex;
  gap:10px;
  align-items:center;
  flex-wrap:wrap;
}
select{
  padding:10px 12px;
  border:1px solid var(--border);
  border-radius:10px;
  background:#fff;
  font-size:14px;
}
button{
  padding:10px 12px;
  border:1px solid var(--border);
  border-radius:10px;
  background:#111827;
  color:white;
  font-weight:600;
  cursor:pointer;
}
.grid{
  display:grid;
  grid-template-columns: 1fr;
  gap:18px;
}
.card{
  background:var(--card);
  border:1px solid var(--border);
  border-radius:var(--radius);
  box-shadow:var(--shadow);
  overflow:hidden;
}
.cardHeader{
  display:flex;
  align-items:center;
  justify-content:space-between;
  padding:14px 16px;
  border-bottom:1px solid var(--border);
}
.cardHeader h2{
  margin:0;
  font-size:16px;
  font-weight:800;
}
.badge{
  display:inline-flex;
  align-items:center;
  gap:6px;
  padding:6px 10px;
  border-radius:999px;
  font-size:12px;
  font-weight:600;
  border:1px solid var(--border);
  background:#f9fafb;
  color:#111827;
}
.badge.pick{ background:#ecfeff; border-color:#a5f3fc;}
.badge.drop{ background:#fef2f2; border-color:#fecaca;}
.badge.repo{ background:#f5f3ff; border-color:#ddd6fe;}
.badge.eta{ background:#f0fdf4; border-color:#bbf7d0;}
.tableWrap{ overflow:auto; }
table{
  width:100%;
  border-collapse:separate;
  border-spacing:0;
  min-width:900px;
}
thead th{
  position:sticky;
  top:0;
  z-index:1;
  background:var(--header);
  color:var(--headerText);
  text-align:left;
  padding:12px 12px;
  font-size:13px;
  font-weight:800;
  border-bottom:1px solid #0b1222;
}
tbody td{
  padding:11px 12px;
  border-bottom:1px solid var(--border);
  vertical-align:top;
  font-size:13px;
}
tbody tr:nth-child(even){ background:#fafafa; }
td.dest{ font-weight:800; }
td.time{ width:90px; font-variant-numeric: tabular-nums; font-weight:800;}
td.obj{ font-weight:500; color:#111827;}
.muted{ color:var(--muted); }
.footer{
  margin-top:18px;
  color:var(--muted);
  font-size:12px;
}
.kv{
  display:flex;
  gap:8px;
  align-items:center;
}
</style>
"""

def _objective_badges(objective: str) -> str:
    s = (objective or "").strip()
    if not s:
        return ""

    badges: List[str] = []

    m = re.search(r"Pickup\s*:\s*(\d+)", s, re.IGNORECASE)
    if m:
        badges.append(f"<span class='badge pick'>↑ Pickup : {m.group(1)}</span>")

    m = re.search(r"Drop\s*Off\s*:\s*(\d+)", s, re.IGNORECASE)
    if m:
        badges.append(f"<span class='badge drop'>↓ Drop Off : {m.group(1)}</span>")

    if re.search(r"Reposition", s, re.IGNORECASE):
        badges.append("<span class='badge repo'>→ Repositionnement</span>")

    m = re.search(r"ETA\s*:?\s*(\d{2}:\d{2}(?::\d{2})?)", s)
    if m:
        badges.append(f"<span class='badge eta'>⏱ ETA : {m.group(1)}</span>")

    if badges:
        return f"<div class='kv'>{''.join(badges)}</div>"

    return html.escape(s)

def render_missions_html(shuttle_name: str, missions: List[Mission], reservation_count: int) -> str:
    rows = []
    for m in missions:
        rows.append(
            "<tr>"
            f"<td class='time'>{html.escape(m.hour)}</td>"
            f"<td>{html.escape(m.depart)}</td>"
            f"<td class='dest'>{html.escape(m.destination)}</td>"
            f"<td class='obj'>{_objective_badges(m.objective)}</td>"
            "</tr>"
        )

    if not rows:
        rows.append("<tr><td colspan='4' class='muted' style='text-align:center;padding:18px;'>Aucune mission</td></tr>")

    return f"""
    <div class="card">
      <div class="cardHeader">
        <h2>Navette {html.escape(shuttle_name)}</h2>
        <span class="badge">{len(missions)} Missions ({reservation_count} Réservations)</span>
      </div>
      <div class="tableWrap">
        <table>
          <thead>
            <tr>
              <th>Heure</th>
              <th>Départ</th>
              <th>Destination</th>
              <th>Objectif</th>
            </tr>
          </thead>
          <tbody>
            {''.join(rows)}
          </tbody>
        </table>
      </div>
    </div>
    """


# ============================================================
# WEB SERVER (FastAPI)
# ============================================================

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from fastapi.responses import JSONResponse

app = FastAPI()

def _normalize_shuttle_param(value: str) -> str:
    v = (value or "").strip().upper()
    if v in {"MB1", "MB5", "ALL"}:
        return v
    return str(CONFIG["web"].get("default_shuttle_view", "ALL")).strip().upper()

@app.get("/state", response_class=JSONResponse)
def state():
    """
    Endpoint léger:
    - retourne uniquement la signature des réservations (sig)
    - si sig change => le front reload la page
    """
    service_day = get_service_date()
    _, _, sig = compute_state_cached(service_day, ttl_seconds=1.0)
    return {"day": str(service_day), "sig": sig, "server_ts": int(time.time())}

@app.get("/", response_class=HTMLResponse)
def index(
    shuttle: str = Query(default=str(CONFIG["web"].get("default_shuttle_view", "ALL")), description="ALL, MB1, MB5"),
):
    shuttle = _normalize_shuttle_param(shuttle)
    service_day = get_service_date()

    missions_by, reservation_counts, sig = compute_state_cached(service_day, ttl_seconds=1.0)

    order = list(CONFIG["fleet"]["shuttles"].keys())
    if shuttle != "ALL":
        order = [shuttle] if shuttle in missions_by else []

    sections = [render_missions_html(s, missions_by.get(s, []), reservation_counts.get(s, 0)) for s in order]

    def selected(v: str) -> str:
        return "selected" if shuttle == v else ""

    poll_ms = int(CONFIG["web"].get("poll_state_ms", 1000))
    pause_ms = int(CONFIG["web"].get("pause_refresh_ms_on_ui", 3000))

    return f"""
    <html>
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>{html.escape(CONFIG["web"].get("title","Planning"))}</title>
        {PAGE_STYLE}
      </head>
      <body>
        <div class="container">
          <div class="topbar">
            <div class="title">
              <h1>{html.escape(CONFIG["web"].get("title","Planification Missions - SQY Flex"))}</h1>
              <div class="subtitle">
                Date de service : <b>{service_day}</b>
              </div>
            </div>

            <form class="controls" method="get" action="/" id="filterForm">
              <select name="shuttle" id="shuttleSelect">
                <option value="ALL" {selected("ALL")}>Toutes (MB1 + MB5)</option>
                <option value="MB1" {selected("MB1")}>MB1 seulement</option>
                <option value="MB5" {selected("MB5")}>MB5 seulement</option>
              </select>
              <button type="submit">Afficher</button>
            </form>
          </div>

          <div class="grid">
            {''.join(sections) if sections else '<div class="card"><div class="cardHeader"><h2>Aucune navette</h2></div><div style="padding:16px;" class="muted">Paramètre shuttle invalide ou aucune donnée.</div></div>'}
          </div>

          <div class="footer">
            Important : Des écarts peuvent apparaître sur cette page si des modifications ont été apportées au back-end du dispatch sans avoir été préalablement communiquées.
          </div>
        </div>

        <script>
          // --- Refresh uniquement si /state.sig change ---
          let currentSig = {html.escape(sig)!r};
          let pauseUntil = 0;

          const POLL_MS = {poll_ms};
          const PAUSE_MS = {pause_ms};

          function nowMs() {{ return Date.now(); }}
          function pauseRefresh() {{ pauseUntil = nowMs() + PAUSE_MS; }}

          // Pause dès qu'on interagit avec la dropdown / le bouton
          const sel = document.getElementById("shuttleSelect");
          const form = document.getElementById("filterForm");

          ["mousedown","touchstart","focus","keydown"].forEach(evt => {{
            sel.addEventListener(evt, pauseRefresh);
          }});
          ["mousedown","touchstart","focus","keydown"].forEach(evt => {{
            form.addEventListener(evt, pauseRefresh);
          }});

          // Optionnel: si tu changes la dropdown, on applique tout de suite (sans attendre clic)
          sel.addEventListener("change", () => {{
            pauseRefresh();
            form.submit();
          }});

          async function pollState() {{
            try {{
              if (nowMs() < pauseUntil) return;

              const r = await fetch("/state", {{ cache: "no-store" }});
              if (!r.ok) return;
              const data = await r.json();

              if (data && data.sig && data.sig !== currentSig) {{
                // conserve le filtre shuttle courant
                const url = new URL(window.location.href);
                const shuttle = url.searchParams.get("shuttle") || "{html.escape(CONFIG["web"].get("default_shuttle_view","ALL"))}";
                window.location.href = "/?shuttle=" + encodeURIComponent(shuttle);
              }}
            }} catch (e) {{
              // silence (pas de spam console)
            }}
          }}

          setInterval(pollState, POLL_MS);
        </script>
      </body>
    </html>
    """


# ============================================================
# ENTRYPOINT (même commande local + plateformes)
# ============================================================

if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    host = "0.0.0.0" if "PORT" in os.environ else "127.0.0.1"

    uvicorn.run(app, host=host, port=port)