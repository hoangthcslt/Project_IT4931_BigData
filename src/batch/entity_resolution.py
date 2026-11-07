"""
Entity Resolution & Spatial Crosswalk Module for NYC Transit Turnstile Data.

Maps raw abbreviated Turnstile station names (e.g., 'GRD CNTRL-42 ST', '34 ST-PENN STA',
'42 ST-PORT AUTH') to canonical records in MTA_StationLocations.csv ('Grand Central - 42 St',
'34 St - Penn Station', '42 St - Port Authority Bus Terminal'), while filtering out non-MTA
systems (such as PATH and Roosevelt Island Tramway).
"""

import csv
import os
import re
from typing import Dict, List, Optional, Set, Tuple

# Non-MTA divisions present in turnstile logs (PATH trains & Roosevelt Island Tram)
NON_MTA_DIVISIONS: Set[str] = {"PTH", "RIT"}

# Non-MTA station names fallback check
NON_MTA_STATIONS: Set[str] = {
    "EXCHANGE PLACE",
    "PAVONIA/NEWPORT",
    "CITY / BUS",
    "CHRISTOPHER ST",  # when PTH
    "HOBOKEN",
    "JOURNAL SQUARE",
    "GROVE STREET",
    "NEWARK HM HE",
    "NEWARK HW BMEBE",
    "NEWARK C",
    "NEWARK BM BW",
    "HARRISON",
    "THIRTY ST",
    "THIRTY THIRD ST",
    "LACKAWANNA",
    "PATH WTC",
    "PATH WTC 2",
    "PATH NEW WTC",
    "TWENTY THIRD ST",
    "14TH STREET",
    "9TH STREET",
    "ORCHARD BEACH",
    "ROOSEVELT ISLND",  # when RIT
}

# Explicit crosswalk mapping from Turnstile raw STATION name to canonical Stop Name substring/key
# in MTA_StationLocations.csv for stations with heavy abbreviations or historical names.
EXPLICIT_STATION_CROSSWALK: Dict[str, str] = {
    "42 ST-GRD CNTRL": "Grand Central - 42 St",
    "GRD CNTRL-42 ST": "Grand Central - 42 St",
    "34 ST-PENN STA": "34 St - Penn Station",
    "34 ST-HUDSON YD": "34 St - 11 Av",
    "42 ST-PORT AUTH": "42 St - Port Authority Bus Terminal",
    "TIMES SQ-42 ST": "Times Sq - 42 St",
    "47-50 STS ROCK": "47-50 Sts - Rockefeller Ctr",
    "59 ST COLUMBUS": "59 St - Columbus Circle",
    "66 ST-LINCOLN": "66 St - Lincoln Center",
    "68ST-HUNTER CO": "68 St - Hunter College",
    "81 ST-MUSEUM": "81 St - Museum of Natural History",
    "116 ST-COLUMBIA": "116 St - Columbia University",
    "137 ST CITY COL": "137 St - City College",
    "163 ST-AMSTERDM": "163 St - Amsterdam Av",
    "161/YANKEE STAD": "161 St - Yankee Stadium",
    "138/GRAND CONC": "138 St - Grand Concourse",
    "149/GRAND CONC": "149 St - Grand Concourse",
    "21 ST-QNSBRIDGE": "21 St - Queensbridge",
    "33 ST-RAWSON ST": "33 St",
    "40 ST LOWERY ST": "40 St",
    "46 ST BLISS ST": "46 St",
    "82 ST-JACKSON H": "82 St - Jackson Hts",
    "90 ST-ELMHURST": "90 St - Elmhurst Av",
    "103 ST-CORONA": "103 St - Corona Plaza",
    "111 ST": "111 St",
    "METS-WILLETS PT": "Mets - Willets Point",
    "FLUSHING-MAIN": "Flushing - Main St",
    "JKSN HT-ROOSVLT": "Jackson Hts - Roosevelt Av",
    "SUTPHIN-ARCHER": "Sutphin Blvd - Archer Av - JFK Airport",
    "JFK JAMAICA CT1": "Sutphin Blvd - Archer Av - JFK Airport",
    "JAMAICA CENTER": "Jamaica Center - Parsons/Archer",
    "JAMAICA-179 ST": "Jamaica - 179 St",
    "JAMAICA-VAN WYC": "Jamaica - Van Wyck",
    "KEW GARDENS": "Kew Gardens - Union Tpke",
    "BRIARWOOD": "Briarwood - Van Wyck Blvd",
    "FOREST HILLS 71": "Forest Hills - 71 Av",
    "WOODHAVEN BLVD": "Woodhaven Blvd",
    "ASTORIA DITMARS": "Astoria - Ditmars Blvd",
    "QUEENSBORO PLZ": "Queensboro Plaza",
    "COURT SQ": "Court Sq",
    "COURT SQ-23 ST": "Court Sq",
    "VERNON-JACKSON": "Vernon Blvd - Jackson Av",
    "HUNTERS PT AV": "Hunters Point Av",
    "ROCKAWAY BLVD": "Rockaway Blvd",
    "AQUEDUCT N.COND": "Aqueduct - N Conduit Av",
    "AQUEDUCT RACETR": "Aqueduct Racetrack",
    "HOWARD BCH JFK": "Howard Beach - JFK Airport",
    "BROAD CHANNEL": "Broad Channel",
    "FAR ROCKAWAY": "Far Rockaway - Mott Av",
    "ROCKAWAY PARK B": "Rockaway Park - Beach 116 St",
    "ATL AV-BARCLAY": "Atlantic Av - Barclays Ctr",
    "JAY ST-METROTEC": "Jay St - MetroTech",
    "BOROUGH HALL": "Borough Hall",
    "COURT ST": "Court St",
    "DEKALB AV": "DeKalb Av",
    "4 AV-9 ST": "4 Av",
    "4AV-9 ST": "4 Av",
    "SMITH-9 ST": "Smith - 9 Sts",
    "15 ST-PROSPECT": "15 St - Prospect Park",
    "PROSPECT PARK": "Prospect Park",
    "CHURCH AV": "Church Av",
    "CONEY IS-STILLW": "Coney Island - Stillwell Av",
    "W 8 ST-AQUARIUM": "W 8 St - NY Aquarium",
    "SHEEPSHEAD BAY": "Sheepshead Bay",
    "BRIGHTON BEACH": "Brighton Beach",
    "OCEAN PKWY": "Ocean Pkwy",
    "CROWN HTS-UTICA": "Crown Hts - Utica Av",
    "FLATBUSH AV-B.C": "Flatbush Av - Brooklyn College",
    "NEWKIRK PLAZA": "Newkirk Plaza",
    "NEWKIRK AV": "Newkirk Av",
    "BEVERLEY ROAD": "Beverley Rd",
    "BEVERLY RD": "Beverly Rd",
    "CORTELYOU RD": "Cortelyou Rd",
    "BEDFORD-NOSTRAN": "Bedford - Nostrand Avs",
    "MYRTLE-WILLOUGH": "Myrtle - Willoughby Avs",
    "MYRTLE-WYCKOFF": "Myrtle - Wyckoff Avs",
    "BROADWAY JCT": "Broadway Jct",
    "CANARSIE-ROCKAW": "Canarsie - Rockaway Pkwy",
    "75 ST-ELDERTS": "75 St",
    "85 ST-FOREST PK": "85 St - Forest Pkwy",
    "WOODSIDE-61 ST": "61 St - Woodside",
    "BUSHWICK AV": "Bushwick Av - Aberdeen St",
    "HOYT-SCHER": "Hoyt - Schermerhorn Sts",
    "CLINTON-WASH AV": "Clinton - Washington Avs",
    "CLASSON AV": "Classon Av",
    "FRANKLIN AV": "Franklin Av",
    "NOSTRAND AV": "Nostrand Av",
    "KINGSTON-THROOP": "Kingston - Throop Avs",
    "UTICA AV": "Utica Av",
    "EUCLID AV": "Euclid Av",
    "GRANT AV": "Grant Av",
    "LEFFERTS BLVD": "Ozone Park - Lefferts Blvd",
    "BROOKLYN BRIDGE": "Brooklyn Bridge - City Hall",
    "CHAMBERS ST": "Chambers St",
    "FULTON ST": "Fulton St",
    "WALL ST": "Wall St",
    "BOWLING GREEN": "Bowling Green",
    "SOUTH FERRY": "South Ferry",
    "WHITEHALL S-FRY": "Whitehall St",
    "RECTOR ST": "Rector St",
    "CORTLANDT ST": "Cortlandt St",
    "WORLD TRADE CTR": "World Trade Center",
    "PARK PLACE": "Park Pl",
    "CANAL ST": "Canal St",
    "SPRING ST": "Spring St",
    "PRINCE ST": "Prince St",
    "B'WAY-LAFAYETTE": "Broadway-Lafayette St",
    "BLEECKER ST": "Bleecker St",
    "ASTOR PL": "Astor Pl",
    "8 ST-NYU": "8 St - NYU",
    "14 ST-UNION SQ": "14 St - Union Sq",
    "14 ST": "14 St",
    "W 4 ST-WASH SQ": "W 4 St",
    "CHRISTOPHER ST": "Christopher St - Sheridan Sq",
    "HOUSTON ST": "Houston St",
    "DELANCEY/ESSEX": "Delancey St",
    "EAST BROADWAY": "East Broadway",
    "GRAND ST": "Grand St",
    "BOWERY": "Bowery",
    "2 AV": "2 Av",
    "1 AV": "1 Av",
    "3 AV": "3 Av",
    "3 AV-149 ST": "3 Av - 149 St",
    "3 AV 138 ST": "3 Av - 138 St",
    "E 143/ST MARY'S": "E 143 St - St Mary's St",
    "E 149 ST": "E 149 St",
    "LONGWOOD AV": "Longwood Av",
    "HUNTS POINT AV": "Hunts Point Av",
    "WHITLOCK AV": "Whitlock Av",
    "ELDER AV": "Elder Av",
    "MORRISON AV-SND": "Morrison Av- Soundview",
    "ST LAWRENCE AV": "St Lawrence Av",
    "PARKCHESTER": "Parkchester",
    "CASTLE HILL AV": "Castle Hill Av",
    "ZEREGA AV": "Zerega Av",
    "WESTCHESTER SQ": "Westchester Sq - E Tremont Av",
    "MIDDLETOWN RD": "Middletown Rd",
    "BUHRE AV": "Buhre Av",
    "PELHAM BAY PARK": "Pelham Bay Park",
    "5 AV/53 ST": "5 Av/53 St",
    "5 AV/59 ST": "5 Av/59 St",
    "5 AVE": "5 Av",
    "LEXINGTON AV/53": "Lexington Av/53 St",
    "LEXINGTON AV/63": "Lexington Av/63 St",
    "59 ST": "59 St",
    "51 ST": "51 St",
    "57 ST-7 AV": "57 St - 7 Av",
    "49 ST": "49 St",
    "7 AV": "7 Av",
    "50 ST": "50 St",
    "23 ST": "23 St",
    "28 ST": "28 St",
    "18 ST": "18 St",
    "34 ST-HERALD SQ": "34 St - Herald Sq",
    "42 ST-BRYANT PK": "42 St - Bryant Pk",
    "72 ST": "72 St",
    "77 ST": "77 St",
    "79 ST": "79 St",
    "86 ST": "86 St",
    "96 ST": "96 St",
    "103 ST": "103 St",
    "110 ST": "110 St",
    "CATHEDRAL PKWY": "Cathedral Pkwy (110 St)",
    "CENTRAL PK N110": "Central Park North (110 St)",
    "116 ST": "116 St",
    "125 ST": "125 St",
    "135 ST": "135 St",
    "145 ST": "145 St",
    "155 ST": "155 St",
    "157 ST": "157 St",
    "167 ST": "167 St",
    "168 ST": "168 St",
    "170 ST": "170 St",
    "174-175 STS": "174-175 Sts",
    "175 ST": "175 St",
    "176 ST": "176 St",
    "181 ST": "181 St",
    "182-183 STS": "182-183 Sts",
    "183 ST": "183 St",
    "190 ST": "190 St",
    "191 ST": "191 St",
    "DYCKMAN ST": "Dyckman St",
    "INWOOD-207 ST": "Inwood - 207 St",
    "207 ST": "207 St",
    "215 ST": "215 St",
    "MARBLE HILL-225": "Marble Hill - 225 St",
    "231 ST": "231 St",
    "238 ST": "238 St",
    "V.CORTLANDT PK": "Van Cortlandt Park - 242 St",
    "HARLEM 148 ST": "Harlem - 148 St",
    "138 ST-3 AVE": "3 Av - 138 St",
    "FORDHAM RD": "Fordham Rd",
    "KINGSBRIDGE RD": "Kingsbridge Rd",
    "BEDFORD PK BLVD": "Bedford Park Blvd",
    "MOSHOLU PKWY": "Mosholu Pkwy",
    "WOODLAWN": "Woodlawn",
    "NORWOOD 205 ST": "Norwood - 205 St",
    "E 180 ST": "E 180 St",
    "W FARMS SQ-E TR": "West Farms Sq - E Tremont Av",
    "174 ST": "174 St",
    "FREEMAN ST": "Freeman St",
    "SIMPSON ST": "Simpson St",
    "INTERVALE AV": "Intervale Av",
    "PROSPECT AV": "Prospect Av",
    "JACKSON AV": "Jackson Av",
    "MORRIS PARK": "Morris Park",
    "PELHAM PKWY": "Pelham Pkwy",
    "BRONX PARK EAST": "Bronx Park East",
    "ALLERTON AV": "Allerton Av",
    "BURKE AV": "Burke Av",
    "GUN HILL RD": "Gun Hill Rd",
    "219 ST": "219 St",
    "225 ST": "225 St",
    "233 ST": "233 St",
    "NEREID AV": "Nereid Av",
    "WAKEFIELD/241": "Wakefield - 241 St",
    "EASTCHSTER/DYRE": "Eastchester - Dyre Av",
    "BAYCHESTER AV": "Baychester Av",
    "ST. GEORGE": "St George",
    "TOMPKINSVILLE": "Tompkinsville",
    "MARCY AV": "Marcy Av",
    "HEWES ST": "Hewes St",
    "LORIMER ST": "Lorimer St",
    "FLUSHING AV": "Flushing Av",
    "MYRTLE AV": "Myrtle Av",
    "KOSCIUSZKO ST": "Kosciuszko St",
    "GATES AV": "Gates Av",
    "HALSEY ST": "Halsey St",
    "CHAUNCEY ST": "Chauncey St",
    "ALABAMA AV": "Alabama Av",
    "VAN SICLEN AV": "Van Siclen Av",
    "CLEVELAND ST": "Cleveland St",
    "NORWOOD AV": "Norwood Av",
    "CRESCENT ST": "Crescent St",
    "CYPRESS HILLS": "Cypress Hills",
    "ELDERTS LANE": "75 St",
    "FOREST PKWY": "85 St - Forest Pkwy",
    "104 ST": "104 St",
    "121 ST": "121 St",
    "SENECA AV": "Seneca Av",
    "FOREST AV": "Forest Av",
    "FRESH POND RD": "Fresh Pond Rd",
    "METROPOLITAN AV": "Middle Village - Metropolitan Av",
    "BEDFORD AV": "Bedford Av",
    "GRAHAM AV": "Graham Av",
    "GRAND ST": "Grand St",
    "MONTROSE AV": "Montrose Av",
    "MORGAN AV": "Morgan Av",
    "JEFFERSON ST": "Jefferson St",
    "DEKALB AV": "DeKalb Av",
    "HALSEY ST": "Halsey St",
    "WILSON AV": "Wilson Av",
    "BUSHWICK AV": "Bushwick Av - Aberdeen St",
    "ATLANTIC AV": "Atlantic Av",
    "SUTTER AV": "Sutter Av",
    "LIVONIA AV": "Livonia Av",
    "NEW LOTS": "New Lots Av",
    "NEW LOTS AV": "New Lots Av",
    "EAST 105 ST": "E 105 St",
    "ROCKAWAY AV": "Rockaway Av",
    "SARATOGA AV": "Saratoga Av",
    "SUTTER AV-RUTLD": "Sutter Av - Rutland Rd",
    "JUNIUS ST": "Junius St",
    "PENNSYLVANIA AV": "Pennsylvania Av",
    "CROWN HTS-UTICA": "Crown Hts - Utica Av",
    "STERLING ST": "Sterling St",
    "WINTHROP ST": "Winthrop St",
    "PRESIDENT ST": "President St",
    "BOTANIC GARDEN": "Botanic Garden",
    "PARK PLACE": "Park Pl",
    "EASTERN PKWY": "Eastern Pkwy - Brooklyn Museum",
    "GRAND ARMY PLZ": "Grand Army Plaza",
    "BERGEN ST": "Bergen St",
    "CARROLL ST": "Carroll St",
    "7 AV": "7 Av",
    "9 AV": "9 Av",
    "FT HAMILTON PKY": "Fort Hamilton Pkwy",
    "50 ST": "50 St",
    "55 ST": "55 St",
    "62 ST": "62 St",
    "71 ST": "71 St",
    "79 ST": "79 St",
    "18 AV": "18 Av",
    "20 AV": "20 Av",
    "BAY PKWY": "Bay Pkwy",
    "25 AV": "25 Av",
    "BAY 50 ST": "Bay 50 St",
    "8 AV": "8 Av",
    "NEW UTRECHT AV": "New Utrecht Av",
    "45 ST": "45 St",
    "53 ST": "53 St",
    "59 ST": "59 St",
    "BAY RIDGE AV": "Bay Ridge Av",
    "77 ST": "77 St",
    "86 ST": "86 St",
    "BAY RIDGE-95 ST": "Bay Ridge - 95 St",
    "36 ST": "36 St",
    "25 ST": "25 St",
    "PROSPECT AV": "Prospect Av",
    "9 ST": "9 St",
    "UNION ST": "Union St",
    "PACIFIC ST": "Atlantic Av - Barclays Ctr",
    "NEVINS ST": "Nevins St",
    "HOYT ST": "Hoyt St",
    "CLARK ST": "Clark St",
    "HIGH ST": "High St",
    "YORK ST": "York St",
    "GREENPOINT AV": "Greenpoint Av",
    "NASSAU AV": "Nassau Av",
    "BROADWAY": "Broadway",
    "FLUSHING AV": "Flushing Av",
    "30 AV": "30 Av",
    "ASTORIA BLVD": "Astoria Blvd",
    "36 AV": "36 Av",
    "39 AV": "39 Av",
    "QUEENS PLAZA": "Queens Plaza",
    "STEINWAY ST": "Steinway St",
    "NORTHERN BLVD": "Northern Blvd",
    "65 ST": "65 St",
    "ROOSEVELT ISLND": "Roosevelt Island",
    "21 ST": "21 St",
    "ELMHURST AV": "Elmhurst Av",
    "GRAND AV-NEWTON": "Grand Av - Newtown",
    "WOODHAVEN BLVD": "Woodhaven Blvd",
    "63 DR-REGO PARK": "63 Dr - Rego Park",
    "67 AV": "67 Av",
    "75 AV": "75 Av",
    "PARSONS BLVD": "Parsons Blvd",
    "169 ST": "169 St",
    "SUTPHIN BLVD": "Sutphin Blvd",
    "34 ST-PENN STA": "34 St - Penn Station",
}


def normalize_station_name(name: str) -> str:
    """Normalize station string using transit domain regex rules."""
    if not name:
        return ""
    s = name.upper().strip()
    s = s.replace("B'WAY", "BROADWAY").replace("BWAY", "BROADWAY")
    s = re.sub(r"(\d+)(ST|AV|RD|AVE)\b", r"\1 \2", s)
    s = re.sub(r"[-/.,()']", " ", s)
    s = re.sub(r"\b0+(\d+)", r"\1", s)
    s = re.sub(r"\b(ST|STREET|STREETS|STS)\b", "ST", s)
    s = re.sub(r"\b(AV|AVE|AVENUE|AVENUES|AVS)\b", "AV", s)
    s = re.sub(r"\b(BLVD|BOULEVARD|BLV)\b", "BLVD", s)
    s = re.sub(r"\b(PKWY|PARKWAY|PKY)\b", "PKWY", s)
    s = re.sub(r"\b(SQ|SQUARE)\b", "SQ", s)
    s = re.sub(r"\b(PK|PARK)\b", "PK", s)
    s = re.sub(r"\b(CTR|CENTER|CNTR)\b", "CTR", s)
    s = re.sub(r"\b(HTS|HEIGHTS|HGTS|HT)\b", "HTS", s)
    s = re.sub(r"\b(RD|ROAD)\b", "RD", s)
    s = re.sub(r"\b(PL|PLACE)\b", "PL", s)
    s = re.sub(r"\b(PLZ|PLAZA)\b", "PLZ", s)
    s = re.sub(r"\b(JCT|JUNCTION)\b", "JCT", s)
    s = re.sub(r"\b(TPKE|TURNPIKE)\b", "TPKE", s)
    s = re.sub(r"\b(HWY|HIGHWAY)\b", "HWY", s)
    s = re.sub(r"\b(BCH|BEACH)\b", "BCH", s)
    s = re.sub(r"\b(FT|FORT)\b", "FT", s)
    s = re.sub(r"\b(MT|MOUNT)\b", "MT", s)
    s = re.sub(r"\b(N|NORTH)\b", "N", s)
    s = re.sub(r"\b(S|SOUTH)\b", "S", s)
    s = re.sub(r"\b(E|EAST)\b", "E", s)
    s = re.sub(r"\b(W|WEST)\b", "W", s)
    s = re.sub(r"\b(GRD|GRAND)\b", "GRAND", s)
    s = re.sub(r"\b(CNTRL|CENTRAL)\b", "CENTRAL", s)
    s = re.sub(r"\b(PENN|PENNSYLVANIA)\b", "PENN", s)
    s = re.sub(r"\b(STA|STATION)\b", "STA", s)
    s = re.sub(r"\b(WASH|WASHINGTON)\b", "WASHINGTON", s)
    s = re.sub(r"\b(LEX|LEXINGTON)\b", "LEXINGTON", s)
    s = re.sub(r"\b(BKLYN|BROOKLYN)\b", "BROOKLYN", s)
    s = re.sub(r"\b(QNS|QUEENS)\b", "QUEENS", s)
    s = re.sub(r"\b(ISLND|IS|ISLAND)\b", "ISLAND", s)
    s = re.sub(r"[^A-Z0-9 ]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


class StationEntityResolver:
    """Resolves raw Turnstile (STATION, LINENAME, DIVISION) into MTA_StationLocations metadata."""

    def __init__(
        self,
        locations_csv_path: str = "data/archive/MTA_StationLocations.csv",
        gtfs_stops_path: str = "data/archive/NYC_TransitSubway/stops.txt",
    ):
        self.locations_csv_path = locations_csv_path
        self.gtfs_stops_path = gtfs_stops_path
        self.stations: List[Dict[str, str]] = []
        self.gtfs_parent_stations: Dict[str, Dict[str, str]] = {}
        self._indexed_locations: List[Tuple[Dict[str, str], str, Set[str], Set[str]]] = []
        self._cache: Dict[Tuple[str, str, str], Optional[Dict[str, str]]] = {}
        self._load_gtfs_parent_stations()
        self._load_locations()

    def _load_gtfs_parent_stations(self) -> None:
        """Load GTFS stops.txt and filter location_type == '1' (Parent Stations) per docs/overview.md."""
        if not os.path.exists(self.gtfs_stops_path):
            return
        with open(self.gtfs_stops_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("location_type", "").strip() == "1":
                    stop_id = row.get("stop_id", "").strip()
                    if stop_id:
                        self.gtfs_parent_stations[stop_id] = {
                            "stop_id": stop_id,
                            "stop_name": row.get("stop_name", "").strip(),
                            "stop_lat": row.get("stop_lat", "").strip(),
                            "stop_lon": row.get("stop_lon", "").strip(),
                            "location_type": "1",
                        }

    def _load_locations(self) -> None:
        if not os.path.exists(self.locations_csv_path):
            raise FileNotFoundError(f"Station locations file not found: {self.locations_csv_path}")

        with open(self.locations_csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                clean_row = {k.strip(): v.strip() for k, v in row.items() if k}
                self.stations.append(clean_row)
                norm_name = normalize_station_name(clean_row["Stop Name"])
                tokens = set(norm_name.split())
                routes = set(re.findall(r"[A-Z0-9]", clean_row.get("Daytime Routes", "").upper()))
                self._indexed_locations.append((clean_row, norm_name, tokens, routes))

    def is_non_mta(self, station: str, division: str) -> bool:
        """Return True if record belongs to non-MTA transit (e.g., PATH or Roosevelt Island Tram)."""
        div = (division or "").strip().upper()
        st = (station or "").strip().upper()
        if div in NON_MTA_DIVISIONS:
            return True
        if st in NON_MTA_STATIONS and div not in {"IRT", "BMT", "IND", "SRT"}:
            return True
        if st == "ORCHARD BEACH":
            return True
        return False

    def resolve(
        self, station: str, linename: str = "", division: str = ""
    ) -> Optional[Dict[str, str]]:
        """
        Resolve a Turnstile (STATION, LINENAME, DIVISION) tuple to its canonical MTA station record.
        Returns a dictionary with keys:
          station_id, complex_id, gtfs_stop_id, stop_name, borough, daytime_routes, latitude, longitude, division
        or None if non-MTA / unresolvable.
        """
        st_clean = (station or "").strip().upper()
        ln_clean = (linename or "").strip().upper()
        div_clean = (division or "").strip().upper()

        if self.is_non_mta(st_clean, div_clean):
            return None

        cache_key = (st_clean, ln_clean, div_clean)
        if cache_key in self._cache:
            return self._cache[cache_key]

        target_name = EXPLICIT_STATION_CROSSWALK.get(st_clean, st_clean)
        norm_target = normalize_station_name(target_name)
        norm_raw = normalize_station_name(st_clean)
        target_tokens = set(norm_target.split())
        raw_tokens = set(norm_raw.split())
        line_routes = set(re.findall(r"[A-Z0-9]", ln_clean))

        best_match: Optional[Dict[str, str]] = None
        best_score: float = -1.0

        for loc, loc_norm, loc_tokens, loc_routes in self._indexed_locations:
            score = 0.0
            route_overlap = len(line_routes & loc_routes)
            div_match = 1 if (div_clean and div_clean == loc.get("Division", "").upper()) else 0

            # 1. Exact normalized match with explicit crosswalk or raw normalized name
            if loc_norm == norm_target or loc_norm == norm_raw:
                score = 100.0 + route_overlap * 10.0 + div_match * 5.0
            # 2. Exact token set equality
            elif target_tokens == loc_tokens or raw_tokens == loc_tokens:
                score = 90.0 + route_overlap * 10.0 + div_match * 5.0
            else:
                # 3. Token overlap & prefix matching
                for q_tokens in (target_tokens, raw_tokens):
                    if not q_tokens or not loc_tokens:
                        continue
                    common = len(q_tokens & loc_tokens)
                    pref = 0
                    for w1 in q_tokens:
                        if any(
                            (len(w1) >= 3 and len(w2) >= 3 and (w1.startswith(w2[:4]) or w2.startswith(w1[:4])))
                            or w1 == w2
                            for w2 in loc_tokens
                        ):
                            pref += 1
                    eff = max(common, pref)
                    min_len = min(len(q_tokens), len(loc_tokens))
                    max_len = max(len(q_tokens), len(loc_tokens))
                    overlap_ratio = eff / max(1, min_len)
                    jaccard = eff / max(1, max_len)

                    # Numeric street/avenue anchor check: if both have numbers, at least one number must match
                    q_nums = {t for t in q_tokens if t.isdigit()}
                    l_nums = {t for t in loc_tokens if t.isdigit()}
                    if q_nums and l_nums and not (q_nums & l_nums):
                        continue

                    if overlap_ratio >= 0.5:
                        candidate_score = (
                            overlap_ratio * 40.0
                            + jaccard * 20.0
                            + route_overlap * 12.0
                            + div_match * 6.0
                        )
                        if candidate_score > score:
                            score = candidate_score

            if score > best_score and score >= 32.0:
                best_score = score
                best_match = loc

        if best_match is None:
            self._cache[cache_key] = None
            return None

        gtfs_id = best_match["GTFS Stop ID"]
        gtfs_parent = self.gtfs_parent_stations.get(gtfs_id, {})
        resolved = {
            "station_id": best_match["Station ID"],
            "complex_id": best_match["Complex ID"],
            "gtfs_stop_id": gtfs_id,
            "gtfs_parent_name": gtfs_parent.get("stop_name", best_match["Stop Name"]),
            "gtfs_location_type": gtfs_parent.get("location_type", "1"),
            "stop_name": best_match["Stop Name"],
            "borough": best_match["Borough"],
            "daytime_routes": best_match["Daytime Routes"],
            "structure": best_match.get("Structure", ""),
            "latitude": float(best_match["GTFS Latitude"]),
            "longitude": float(best_match["GTFS Longitude"]),
            "division": best_match["Division"],
        }
        self._cache[cache_key] = resolved
        return resolved

    def build_lookup_table(
        self, turnstile_keys: List[Tuple[str, str, str]]
    ) -> List[Dict[str, object]]:
        """Build a flat lookup list of dicts suitable for creating a Spark DataFrame."""
        rows = []
        for st, ln, div in turnstile_keys:
            res = self.resolve(st, ln, div)
            if res is not None:
                rows.append(
                    {
                        "raw_station": st,
                        "raw_linename": ln,
                        "raw_division": div,
                        **res,
                    }
                )
        return rows


if __name__ == "__main__":
    import glob

    resolver = StationEntityResolver()
    sample_files = sorted(glob.glob("data/archive/TurnstileData/turnstile_*.txt"))
    check_files = [sample_files[0], sample_files[len(sample_files) // 2], sample_files[-1]] if sample_files else []

    unique_keys: Set[Tuple[str, str, str]] = set()
    for path in check_files:
        with open(path, mode="r", encoding="utf-8", errors="ignore") as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if len(row) >= 6:
                    unique_keys.add((row[3].strip(), row[4].strip(), row[5].strip()))

    mta_keys = [k for k in unique_keys if not resolver.is_non_mta(k[0], k[2])]
    non_mta_keys = [k for k in unique_keys if resolver.is_non_mta(k[0], k[2])]

    matched = 0
    unmatched = []
    for st, ln, div in sorted(mta_keys):
        res = resolver.resolve(st, ln, div)
        if res:
            matched += 1
        else:
            unmatched.append((st, ln, div))

    match_rate = (matched / len(mta_keys) * 100.0) if mta_keys else 0.0
    print(f"Total unique keys inspected: {len(unique_keys)}")
    print(f"Non-MTA filtered out (PATH/RIT): {len(non_mta_keys)}")
    print(f"Valid MTA Subway station keys: {len(mta_keys)}")
    print(f"Successfully resolved: {matched}/{len(mta_keys)} ({match_rate:.2f}%)")
    if unmatched:
        print("Unmatched keys:", unmatched)
