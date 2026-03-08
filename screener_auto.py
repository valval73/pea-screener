"""
=============================================================================
  LA FIRME — SCREENER PRO v3.1
  Univers elargi — tickers corriges — rotation quotidienne complete
=============================================================================
"""

import yfinance as yf
import pandas as pd
import numpy as np
import json, os, time, csv, math, sys
from datetime import datetime, timedelta
from pathlib import Path

CONFIG = {
    "max_par_session" : 50,   # 50 actions/jour = tout l univers en 3-4 jours
    "delai_requete"   : 1.0,
    "delai_retry"     : 3.0,
    "max_retries"     : 2,
    "seuil_candidat"  : 55,
    "seuil_pepite"    : 75,
    "refresh_jours"   : 4,    # re-analyser apres 4 jours = rotation complete
    "version"         : "3.1",
}

# =============================================================================
# UNIVERS PEA — 160 actions, tickers verifies Yahoo Finance
# =============================================================================
UNIVERS = [
    # ── CAC 40 (38 valeurs) ───────────────────────────────────────────────────
    ("AI.PA",     "Air Liquide",          "Materiaux",  "PA"),
    ("AIR.PA",    "Airbus",               "Industrie",  "PA"),
    ("ALO.PA",    "Alstom",               "Industrie",  "PA"),
    ("MT.AS",     "ArcelorMittal",        "Materiaux",  "AS"),
    ("CS.PA",     "AXA",                  "Finance",    "PA"),
    ("BNP.PA",    "BNP Paribas",          "Finance",    "PA"),
    ("EN.PA",     "Bouygues",             "Industrie",  "PA"),
    ("CAP.PA",    "Capgemini",            "Tech",       "PA"),
    ("CA.PA",     "Carrefour",            "Conso",      "PA"),
    ("ACA.PA",    "Credit Agricole",      "Finance",    "PA"),
    ("BN.PA",     "Danone",               "Conso",      "PA"),
    ("DSY.PA",    "Dassault Systemes",    "Tech",       "PA"),
    ("ENGI.PA",   "Engie",                "Energie",    "PA"),
    ("EL.PA",     "EssilorLuxottica",     "Sante",      "PA"),
    ("RMS.PA",    "Hermes",               "Conso",      "PA"),
    ("KER.PA",    "Kering",               "Conso",      "PA"),
    ("LR.PA",     "Legrand",              "Industrie",  "PA"),
    ("OR.PA",     "L Oreal",              "Conso",      "PA"),
    ("MC.PA",     "LVMH",                 "Conso",      "PA"),
    ("ML.PA",     "Michelin",             "Industrie",  "PA"),
    ("ORA.PA",    "Orange",               "Telecom",    "PA"),
    ("RI.PA",     "Pernod Ricard",        "Conso",      "PA"),
    ("PUB.PA",    "Publicis",             "Tech",       "PA"),
    ("RNO.PA",    "Renault",              "Industrie",  "PA"),
    ("SAF.PA",    "Safran",               "Industrie",  "PA"),
    ("SGO.PA",    "Saint-Gobain",         "Materiaux",  "PA"),
    ("SAN.PA",    "Sanofi",               "Sante",      "PA"),
    ("SU.PA",     "Schneider Electric",   "Industrie",  "PA"),
    ("GLE.PA",    "Societe Generale",     "Finance",    "PA"),
    ("STLAM.MI",  "Stellantis",           "Industrie",  "MI"),
    ("STM.MI",    "STMicroelectronics",   "Tech",       "MI"),
    ("TEP.PA",    "Teleperformance",      "Tech",       "PA"),
    ("HO.PA",     "Thales",               "Industrie",  "PA"),
    ("TTE.PA",    "TotalEnergies",        "Energie",    "PA"),
    ("URW.PA",    "Unibail-Rodamco",      "Immo",       "PA"),
    ("VIE.PA",    "Veolia",               "Utilities",  "PA"),
    ("VIV.PA",    "Vivendi",              "Telecom",    "PA"),
    ("WLN.PA",    "Worldline",            "Tech",       "PA"),
    # ── SBF 120 France Mid Cap ────────────────────────────────────────────────
    ("AF.PA",     "Air France-KLM",       "Industrie",  "PA"),
    ("BB.PA",     "BIC",                  "Conso",      "PA"),
    ("COFA.PA",   "Coface",               "Finance",    "PA"),
    ("FGR.PA",    "Eiffage",              "Industrie",  "PA"),
    ("ERF.PA",    "Eurofins Scientific",  "Sante",      "PA"),
    ("GTT.PA",    "Gaztransport",         "Energie",    "PA"),
    ("MF.PA",     "Wendel",               "Finance",    "PA"),
    ("RCO.PA",    "Remy Cointreau",       "Conso",      "PA"),
    ("RXL.PA",    "Rexel",                "Industrie",  "PA"),
    ("SFCA.PA",   "Sopra Steria",         "Tech",       "PA"),
    ("SMCP.PA",   "SMCP",                 "Conso",      "PA"),
    ("SOI.PA",    "Soitec",               "Tech",       "PA"),
    ("SPIE.PA",   "SPIE",                 "Industrie",  "PA"),
    ("TFI.PA",    "TF1",                  "Telecom",    "PA"),
    ("TKO.PA",    "Tikehau Capital",      "Finance",    "PA"),
    ("TNG.PA",    "Trigano",              "Conso",      "PA"),
    ("VK.PA",     "Vallourec",            "Materiaux",  "PA"),
    ("LACR.PA",   "Lacroix",              "Industrie",  "PA"),
    ("VIRP.PA",   "Virbac",               "Sante",      "PA"),
    ("LNA.PA",    "LNA Sante",            "Sante",      "PA"),
    ("MERY.PA",   "Mersen",               "Industrie",  "PA"),
    ("SWP.PA",    "Sword Group",          "Tech",       "PA"),
    ("FII.PA",    "Fonciere INEA",        "Immo",       "PA"),
    ("ELIS.PA",   "Elis",                 "Industrie",  "PA"),
    ("FNAC.PA",   "Fnac Darty",           "Conso",      "PA"),
    ("DG.PA",     "Vinci",                "Industrie",  "PA"),
    ("EXA.PA",    "Exacompta",            "Industrie",  "PA"),
    ("OPM.PA",    "OPmobility",           "Industrie",  "PA"),
    # ── Amsterdam ────────────────────────────────────────────────────────────
    ("ASML.AS",   "ASML Holding",         "Tech",       "AS"),
    ("ADYEN.AS",  "Adyen",                "Tech",       "AS"),
    ("INGA.AS",   "ING Groep",            "Finance",    "AS"),
    ("PHIA.AS",   "Philips",              "Sante",      "AS"),
    ("RAND.AS",   "Randstad",             "Industrie",  "AS"),
    ("WKL.AS",    "Wolters Kluwer",       "Tech",       "AS"),
    ("ABN.AS",    "ABN AMRO",             "Finance",    "AS"),
    ("AGN.AS",    "Aegon",                "Finance",    "AS"),
    ("AKZA.AS",   "Akzo Nobel",           "Materiaux",  "AS"),
    ("DSMF.AS",   "DSM-Firmenich",        "Materiaux",  "AS"),
    ("HEIA.AS",   "Heineken",             "Conso",      "AS"),
    ("NN.AS",     "NN Group",             "Finance",    "AS"),
    ("IMCD.AS",   "IMCD",                 "Materiaux",  "AS"),
    ("UMG.AS",    "Universal Music",      "Telecom",    "AS"),
    ("PRX.AS",    "Prosus",               "Tech",       "AS"),
    ("OCI.AS",    "OCI Global",           "Materiaux",  "AS"),
    ("BESI.AS",   "BE Semiconductor",     "Tech",       "AS"),
    ("LIGHT.AS",  "Signify",              "Industrie",  "AS"),
    # ── Bruxelles ────────────────────────────────────────────────────────────
    ("UCB.BR",    "UCB SA",               "Sante",      "BR"),
    ("SOLB.BR",   "Solvay",               "Materiaux",  "BR"),
    ("ABI.BR",    "AB InBev",             "Conso",      "BR"),
    ("COLR.BR",   "Colruyt",              "Conso",      "BR"),
    ("GBLB.BR",   "GBL",                  "Finance",    "BR"),
    ("GLPG.BR",   "Galapagos",            "Sante",      "BR"),
    ("ACKB.BR",   "Ackermans",            "Finance",    "BR"),
    ("ARGX.BR",   "argenx",               "Sante",      "BR"),
    ("KBC.BR",    "KBC Groupe",           "Finance",    "BR"),
    ("AED.BR",    "Aedifica",             "Immo",       "BR"),
    ("COFB.BR",   "Cofinimmo",            "Immo",       "BR"),
    # ── Allemagne ────────────────────────────────────────────────────────────
    ("SAP.DE",    "SAP SE",               "Tech",       "DE"),
    ("SIE.DE",    "Siemens AG",           "Industrie",  "DE"),
    ("ALV.DE",    "Allianz SE",           "Finance",    "DE"),
    ("MUV2.DE",   "Munich Re",            "Finance",    "DE"),
    ("DTE.DE",    "Deutsche Telekom",     "Telecom",    "DE"),
    ("BMW.DE",    "BMW AG",               "Industrie",  "DE"),
    ("VOW3.DE",   "Volkswagen",           "Industrie",  "DE"),
    ("BASF.DE",   "BASF SE",              "Materiaux",  "DE"),
    ("DB1.DE",    "Deutsche Boerse",      "Finance",    "DE"),
    ("HEN3.DE",   "Henkel",               "Conso",      "DE"),
    ("FRE.DE",    "Fresenius",            "Sante",      "DE"),
    ("MRK.DE",    "Merck KGaA",           "Sante",      "DE"),
    ("SHL.DE",    "Siemens Healthineers", "Sante",      "DE"),
    ("ADS.DE",    "Adidas",               "Conso",      "DE"),
    ("BAYN.DE",   "Bayer AG",             "Sante",      "DE"),
    ("IFX.DE",    "Infineon",             "Tech",       "DE"),
    ("RWE.DE",    "RWE AG",               "Energie",    "DE"),
    ("HNR1.DE",   "Hannover Rueck",       "Finance",    "DE"),
    ("DHL.DE",    "Deutsche Post",        "Industrie",  "DE"),
    ("MBG.DE",    "Mercedes-Benz",        "Industrie",  "DE"),
    ("DHER.DE",   "Delivery Hero",        "Tech",       "DE"),
    ("ZAL.DE",    "Zalando",              "Conso",      "DE"),
    ("AIXA.DE",   "Aixtron",              "Tech",       "DE"),
    ("EVK.DE",    "Evonik",               "Materiaux",  "DE"),
    # ── Danemark ─────────────────────────────────────────────────────────────
    ("DSV.CO",    "DSV A/S",              "Industrie",  "CO"),
    ("NOVO-B.CO", "Novo Nordisk",         "Sante",      "CO"),
    ("CARL-B.CO", "Carlsberg",            "Conso",      "CO"),
    ("ORSTED.CO", "Orsted",               "Energie",    "CO"),
    ("MAERSK-B.CO","Maersk",             "Industrie",  "CO"),
    ("GN.CO",     "GN Store Nord",        "Tech",       "CO"),
    ("DEMANT.CO", "Demant",               "Sante",      "CO"),
    ("RBREW.CO",  "Royal Unibrew",        "Conso",      "CO"),
    # ── Finlande ─────────────────────────────────────────────────────────────
    ("NOKIA.HE",  "Nokia Oyj",            "Tech",       "HE"),
    ("NESTE.HE",  "Neste Oyj",            "Energie",    "HE"),
    ("SAMPO.HE",  "Sampo",                "Finance",    "HE"),
    ("STERV.HE",  "Stora Enso",           "Materiaux",  "HE"),
    ("KNEBV.HE",  "Kone",                 "Industrie",  "HE"),
    ("FORTUM.HE", "Fortum",               "Utilities",  "HE"),
    ("OUT1V.HE",  "Outokumpu",            "Materiaux",  "HE"),
    ("TIETO.HE",  "TietoEVRY",            "Tech",       "HE"),
    ("METSO.HE",  "Metso",                "Industrie",  "HE"),
    # ── Suede ────────────────────────────────────────────────────────────────
    ("INVE-B.ST", "Investor AB",          "Finance",    "ST"),
    ("ATCO-A.ST", "Atlas Copco",          "Industrie",  "ST"),
    ("VOLV-B.ST", "Volvo AB",             "Industrie",  "ST"),
    ("ERIC-B.ST", "Ericsson",             "Tech",       "ST"),
    ("SEB-A.ST",  "SEB AB",               "Finance",    "ST"),
    ("SWED-A.ST", "Swedbank",             "Finance",    "ST"),
    ("SHB-A.ST",  "Handelsbanken",        "Finance",    "ST"),
    ("SAND.ST",   "Sandvik",              "Industrie",  "ST"),
    ("ALFA.ST",   "Alfa Laval",           "Industrie",  "ST"),
    ("ESSITY-B.ST","Essity",              "Sante",      "ST"),
    ("SKF-B.ST",  "SKF",                  "Industrie",  "ST"),
    ("HM-B.ST",   "H&M",                  "Conso",      "ST"),
    ("TELIA.ST",  "Telia",                "Telecom",    "ST"),
    ("SECU-B.ST", "Securitas",            "Industrie",  "ST"),
    ("ABB.ST",    "ABB Ltd",              "Industrie",  "ST"),
    # ── Norvege (EEE = eligible PEA) ─────────────────────────────────────────
    ("EQNR.OL",   "Equinor",              "Energie",    "OL"),
    ("DNB.OL",    "DNB Bank",             "Finance",    "OL"),
    ("TEL.OL",    "Telenor",              "Telecom",    "OL"),
    ("MOWI.OL",   "Mowi",                 "Conso",      "OL"),
    ("YAR.OL",    "Yara International",   "Materiaux",  "OL"),
    ("NHY.OL",    "Norsk Hydro",          "Materiaux",  "OL"),
    ("ORK.OL",    "Orkla",                "Conso",      "OL"),
    ("SALM.OL",   "SalMar",               "Conso",      "OL"),
    ("STB.OL",    "Storebrand",           "Finance",    "OL"),
    ("SRBNK.OL",  "Sparebank 1 SR-Bank",  "Finance",    "OL"),
    ("LSG.OL",    "Leroy Seafood",        "Conso",      "OL"),
    ("AKRBP.OL",  "Aker BP",              "Energie",    "OL"),
    # ── Portugal ─────────────────────────────────────────────────────────────
    ("EDP.LS",    "EDP SA",               "Utilities",  "LS"),
    ("GALP.LS",   "Galp Energia",         "Energie",    "LS"),
    ("EDPR.LS",   "EDP Renovaveis",       "Energie",    "LS"),
    ("BCP.LS",    "Banco Comercial",      "Finance",    "LS"),
    ("NOS.LS",    "NOS SGPS",             "Telecom",    "LS"),
    ("EGL.LS",    "Greenvolt",            "Energie",    "LS"),
    ("CTT.LS",    "CTT Correios",         "Industrie",  "LS"),
    # ── Espagne ──────────────────────────────────────────────────────────────
    ("ITX.MC",    "Inditex",              "Conso",      "MC"),
    ("IBE.MC",    "Iberdrola",            "Utilities",  "MC"),
    ("SAN.MC",    "Banco Santander",      "Finance",    "MC"),
    ("BBVA.MC",   "BBVA",                 "Finance",    "MC"),
    ("REP.MC",    "Repsol",               "Energie",    "MC"),
    ("TEF.MC",    "Telefonica",           "Telecom",    "MC"),
    ("AMS.MC",    "Amadeus IT",           "Tech",       "MC"),
    ("CABK.MC",   "CaixaBank",            "Finance",    "MC"),
    ("FER.MC",    "Ferrovial",            "Industrie",  "MC"),
    ("MAP.MC",    "Mapfre",               "Finance",    "MC"),
    ("CLNX.MC",   "Cellnex",              "Telecom",    "MC"),
    ("ENG.MC",    "Enagas",               "Energie",    "MC"),
    ("REE.MC",    "Red Electrica",        "Utilities",  "MC"),
    ("MRL.MC",    "Merlin Properties",    "Immo",       "MC"),
    ("COL.MC",    "Inmobiliaria Colonial", "Immo",      "MC"),
    ("ACX.MC",    "Acerinox",             "Materiaux",  "MC"),
    ("ACS.MC",    "ACS Actividades",      "Industrie",  "MC"),
    ("IDR.MC",    "Indra",                "Tech",       "MC"),
    ("MEL.MC",    "Melia Hotels",         "Conso",      "MC"),
    # ── SRD France — actions manquantes ajoutees ────────────────────────────
    ("ADP.PA",      "Aeroports de Paris",       "Industrie",   "PA"),
    ("AMUN.PA",     "Amundi",                   "Finance",     "PA"),
    ("ANTIN.PA",    "Antin Infrastructure",     "Finance",     "PA"),
    ("ARGAN.PA",    "Argan SIIC",               "Immo",        "PA"),
    ("DIM.PA",      "Sartorius Stedim",         "Sante",       "PA"),
    ("SK.PA",       "SEB SA",                   "Conso",       "PA"),
    ("SW.PA",       "Sodexo",                   "Industrie",   "PA"),
    ("RUI.PA",      "Rubis",                    "Energie",     "PA"),
    ("GDS.PA",      "Ramsay Sante",             "Sante",       "PA"),
    ("GET.PA",      "Getlink",                  "Industrie",   "PA"),
    ("GFC.PA",      "Gecina",                   "Immo",        "PA"),
    ("LI.PA",       "Klepierre",                "Immo",        "PA"),
    ("IPN.PA",      "Ipsen",                    "Sante",       "PA"),
    ("NEX.PA",      "Nexans",                   "Industrie",   "PA"),
    ("DEC.PA",      "JCDecaux",                 "Telecom",     "PA"),
    ("MMB.PA",      "Lagardere",                "Telecom",     "PA"),
    ("BOL.PA",      "Bollore",                  "Finance",     "PA"),
    ("CDI.PA",      "Christian Dior",           "Conso",       "PA"),
    ("FR.PA",       "Valeo",                    "Industrie",   "PA"),
    ("STEF.PA",     "Stef",                     "Industrie",   "PA"),
    ("RBT.PA",      "Robertet",                 "Conso",       "PA"),
    ("CARM.PA",     "Carmila",                  "Immo",        "PA"),
    ("VCT.PA",      "Vicat",                    "Materiaux",   "PA"),
    ("PEUG.PA",     "Peugeot Invest",           "Finance",     "PA"),
    ("LSS.PA",      "Lectra",                   "Tech",        "PA"),
    ("FRVIA.PA",    "Forvia",                   "Industrie",   "PA"),
    ("ATE.PA",      "Alten",                    "Tech",        "PA"),
    ("PLX.PA",      "Pluxee",                   "Industrie",   "PA"),
    ("74SW.PA",     "74Software",               "Tech",        "PA"),
    ("ELI.PA",      "Elis",                     "Industrie",   "PA"),
    ("IDLG.PA",     "ID Logistics",             "Industrie",   "PA"),
    ("IPS.PA",      "Ipsos",                    "Tech",        "PA"),
    ("ABVX.PA",     "Abivax",                   "Sante",       "PA"),
    ("BIM.PA",      "BioMerieux",               "Sante",       "PA"),
    ("ASY.PA",      "Assystem",                 "Industrie",   "PA"),
    ("LISI.PA",     "Lisi",                     "Industrie",   "PA"),
    ("TFF.PA",      "TFF Group",                "Conso",       "PA"),
    # ── Italie ───────────────────────────────────────────────────────────────
    ("ENI.MI",    "ENI SpA",              "Energie",    "MI"),
    ("ENEL.MI",   "Enel SpA",             "Utilities",  "MI"),
    ("ISP.MI",    "Intesa Sanpaolo",      "Finance",    "MI"),
    ("UCG.MI",    "UniCredit",            "Finance",    "MI"),
    ("LDO.MI",    "Leonardo",             "Industrie",  "MI"),
    ("PRY.MI",    "Prysmian",             "Industrie",  "MI"),
    ("G.MI",      "Generali",             "Finance",    "MI"),
    ("BAMI.MI",   "Banco BPM",            "Finance",    "MI"),
    ("REC.MI",    "Recordati",            "Sante",      "MI"),
    ("TIT.MI",    "Telecom Italia",       "Telecom",    "MI"),
    ("AZM.MI",    "Azimut",               "Finance",    "MI"),
    ("RACE.MI",   "Ferrari",              "Conso",      "MI"),
    ("MONC.MI",   "Moncler",              "Conso",      "MI"),
    ("CPR.MI",    "Campari",              "Conso",      "MI"),
    ("SRG.MI",    "Snam",                 "Energie",    "MI"),
    ("TRN.MI",    "Terna",                "Utilities",  "MI"),
    ("A2A.MI",    "A2A SpA",              "Utilities",  "MI"),
    ("INW.MI",    "Inwit",                "Telecom",    "MI"),
    ("BMED.MI",   "Banca Mediolanum",     "Finance",    "MI"),
    ("CNHI.MI",   "CNH Industrial",       "Industrie",  "MI"),
    # ── Corrections et ajouts verifies Yahoo Finance ──────────────────────────
    ("BVI.PA",   "Bureau Veritas",       "Industrie",  "PA"),
    ("ENXB.PA",  "Edenred",              "Tech",       "PA"),
    ("LOTB.BR",  "Lotus Bakeries",       "Conso",      "BR"),
    ("NEOEP.PA", "Neoen",                "Energie",    "PA"),
    ("EOAN.DE",  "E.ON SE",              "Utilities",  "DE"),
    ("BEKB.BR",  "Bekaert",              "Materiaux",  "BR"),
    ("MELE.BR",  "Melexis",              "Tech",       "BR"),
    ("UMI.BR",   "Umicore",              "Materiaux",  "BR"),
]

# Dedoublonnage
seen = set()
UNIVERS_PROPRE = []
for item in UNIVERS:
    if item[0] not in seen:
        seen.add(item[0])
        UNIVERS_PROPRE.append(item)

RESULTATS_DIR  = Path("resultats")
SCREENER_CSV   = RESULTATS_DIR / "screener_complet.csv"
CANDIDATS_JSON = RESULTATS_DIR / "candidats.json"
README_MD      = Path("README.md")
RESULTATS_DIR.mkdir(exist_ok=True)

def charger_etat():
    etat = {}
    if SCREENER_CSV.exists():
        try:
            df = pd.read_csv(SCREENER_CSV)
            for _, row in df.iterrows():
                t = str(row.get("Ticker",""))
                d = str(row.get("Date_Analyse",""))
                if t and d and d != "nan":
                    etat[t] = d
        except Exception:
            pass
    return etat

def selectionner_batch(etat, max_actions):
    today  = datetime.now().date()
    jamais = []
    vieux  = []
    for ticker, nom, secteur, bourse in UNIVERS_PROPRE:
        if ticker not in etat:
            jamais.append((ticker, nom, secteur, bourse))
        else:
            try:
                last = datetime.strptime(etat[ticker], "%Y-%m-%d").date()
                age  = (today - last).days
                if age >= CONFIG["refresh_jours"]:
                    vieux.append((ticker, nom, secteur, bourse, age))
            except Exception:
                jamais.append((ticker, nom, secteur, bourse))
    vieux.sort(key=lambda x: x[4], reverse=True)
    batch = jamais[:max_actions]
    if len(batch) < max_actions:
        reste = max_actions - len(batch)
        batch += [(t,n,s,b) for t,n,s,b,_ in vieux[:reste]]
    return batch[:max_actions]

def safe(val, default=None):
    try:
        if val is None: return default
        f = float(val)
        if math.isnan(f) or math.isinf(f): return default
        return f
    except Exception:
        return default

def piotroski_f_score(info, fin):
    score = 0
    try:
        roa = safe(info.get("returnOnAssets"))
        cfo = None
        if fin is not None and not fin.empty:
            cols = list(fin.columns)
            if cols:
                c0 = fin[cols[0]]
                cfo = safe(c0.get("Total Cash From Operating Activities") or c0.get("Operating Cash Flow"))
        if roa and roa > 0: score += 1
        if cfo and cfo > 0: score += 1
        if roa and roa > 0.03: score += 1
        ta = safe(info.get("totalAssets"))
        td = safe(info.get("totalDebt"))
        if ta and td is not None and ta > 0:
            if td / ta < 0.5: score += 1
        cur = safe(info.get("currentRatio"))
        if cur and cur > 1: score += 1
        gm = safe(info.get("grossMargins"))
        if gm and gm > 0: score += 1
        rev = safe(info.get("totalRevenue"))
        if rev and ta and ta > 0 and rev/ta > 0.1: score += 1
        score += 1  # bonus de base
    except Exception:
        pass
    return score

def altman_z(info):
    try:
        ta = safe(info.get("totalAssets"))
        if not ta or ta <= 0: return None
        cap  = safe(info.get("marketCap")) or 0
        tl   = safe(info.get("totalDebt")) or 0
        rev  = safe(info.get("totalRevenue")) or 0
        ebit = safe(info.get("ebit")) or 0
        re   = safe(info.get("retainedEarnings")) or 0
        ca   = safe(info.get("currentAssets")) or 0
        cl   = safe(info.get("currentLiabilities")) or 0
        wc   = ca - cl
        x1 = wc / ta
        x2 = re / ta
        x3 = ebit / ta
        x4 = cap / max(tl, 1)
        x5 = rev / ta
        return round(1.2*x1 + 1.4*x2 + 3.3*x3 + 0.6*x4 + x5, 2)
    except Exception:
        return None

def accruals(info, cf):
    try:
        ni = safe(info.get("netIncomeToCommon"))
        ta = safe(info.get("totalAssets"))
        cfo = None
        if cf is not None and not cf.empty:
            cols = list(cf.columns)
            if cols:
                c0 = cf[cols[0]]
                cfo = safe(c0.get("Total Cash From Operating Activities") or c0.get("Operating Cash Flow"))
        if ni and cfo and ta and ta > 0:
            return round((ni - cfo) / ta, 4)
    except Exception:
        pass
    return None

def dist_52s(hist):
    try:
        if hist is None or len(hist) < 10: return None
        ph = hist["Close"].iloc[-min(252, len(hist)):].max()
        if ph > 0:
            return round((hist["Close"].iloc[-1] / ph - 1) * 100, 1)
    except Exception:
        pass
    return None

def rev_proxy(info):
    try:
        fpe = safe(info.get("forwardPE"))
        tpe = safe(info.get("trailingPE"))
        if fpe and tpe and tpe > 0 and fpe > 0:
            return round(fpe / tpe, 3)
    except Exception:
        pass
    return None

def liquidite(info):
    try:
        vol  = safe(info.get("averageVolume"))
        prix = safe(info.get("currentPrice") or info.get("regularMarketPrice"))
        if vol and prix:
            v = vol * prix
            if v > 5_000_000:  return "OK"
            if v > 500_000:    return "ATTENTION"
            return "ILLIQUIDE"
    except Exception:
        pass
    return None

def score_clip(val, lo, hi, inv=False):
    if val is None: return 50
    if inv: val, lo, hi = -val, -hi, -lo
    return max(0, min(100, (val - lo) / (hi - lo) * 100))

def fetch_action(ticker, nom, secteur, bourse, hist_all):
    for attempt in range(CONFIG["max_retries"]):
        try:
            t    = yf.Ticker(ticker)
            info = t.info
            if not info or safe(info.get("regularMarketPrice")) is None:
                if attempt < CONFIG["max_retries"] - 1:
                    time.sleep(CONFIG["delai_retry"])
                    continue
                return None

            hist = t.history(period="2y", auto_adjust=True)
            hist_all[ticker] = hist if not hist.empty else None

            try:
                fin = t.financials
                cf  = t.cashflow
            except Exception:
                fin = cf = None

            prix_v  = safe(info.get("currentPrice") or info.get("regularMarketPrice"))
            devise  = info.get("currency", "EUR")
            cap_raw = safe(info.get("marketCap"))
            cap_lab = "Large" if (cap_raw or 0) >= 10e9 else "Mid" if (cap_raw or 0) >= 2e9 else "Small"

            roe  = safe(info.get("returnOnEquity"))
            if roe: roe = round(roe * 100, 1)
            roa  = safe(info.get("returnOnAssets"))
            if roa: roa = round(roa * 100, 1)
            gm   = safe(info.get("grossMargins"))
            if gm: gm = round(gm * 100, 1)
            pm   = safe(info.get("profitMargins"))
            if pm: pm = round(pm * 100, 1)
            rg   = safe(info.get("revenueGrowth"))
            if rg: rg = round(rg * 100, 1)
            eg   = safe(info.get("earningsGrowth"))
            if eg: eg = round(eg * 100, 1)

            # ROIC proxy
            eq = safe(info.get("totalStockholdersEquity"))
            td = safe(info.get("totalDebt"))
            roic = None
            if roe and eq and td is not None:
                ce = eq + (td or 0)
                if ce > 0: roic = round(roe * eq / ce, 1)

            ev_eb = safe(info.get("enterpriseToEbitda"))
            if ev_eb: ev_eb = round(ev_eb, 1)
            pe_v  = safe(info.get("trailingPE"))
            if pe_v: pe_v = round(pe_v, 1)
            fpe_v = safe(info.get("forwardPE"))
            if fpe_v: fpe_v = round(fpe_v, 1)
            pb_v  = safe(info.get("priceToBook"))
            if pb_v: pb_v = round(pb_v, 1)
            ps_v  = safe(info.get("priceToSalesTrailing12Months"))
            if ps_v: ps_v = round(ps_v, 1)
            pfcf_v = safe(info.get("priceToFreeCashflows"))
            if pfcf_v: pfcf_v = round(pfcf_v, 1)
            fcf = safe(info.get("freeCashflow"))
            fcy = round(fcf / cap_raw * 100, 1) if fcf and cap_raw and cap_raw > 0 else None
            div_y = safe(info.get("dividendYield"))
            if div_y: div_y = round(div_y * 100, 2)

            # Momentum
            mom12 = mom6 = mom3 = None
            if hist is not None and not hist.empty and len(hist) > 20:
                c = hist["Close"]
                try:
                    if len(c) >= 252: mom12 = round((c.iloc[-21] / c.iloc[-252] - 1) * 100, 1)
                    if len(c) >= 120: mom6  = round((c.iloc[-1]  / c.iloc[-120] - 1) * 100, 1)
                    if len(c) >= 60:  mom3  = round((c.iloc[-1]  / c.iloc[-60]  - 1) * 100, 1)
                except Exception: pass

            ebitda = safe(info.get("ebitda"))
            deb_eb = round((td or 0) / ebitda, 2) if ebitda and td is not None and ebitda > 0 else None
            cur_r  = safe(info.get("currentRatio"))
            if cur_r: cur_r = round(cur_r, 2)
            beta_v = safe(info.get("beta"))
            if beta_v: beta_v = round(beta_v, 2)

            vol30 = None
            if hist is not None and not hist.empty and len(hist) >= 30:
                try:
                    vol30 = round(hist["Close"].pct_change().dropna().tail(30).std() * np.sqrt(252) * 100, 1)
                except Exception: pass

            pio  = piotroski_f_score(info, fin)
            alt  = altman_z(info)
            acc  = accruals(info, cf)
            d52  = dist_52s(hist)
            rp   = rev_proxy(info)
            liq  = liquidite(info)

            # Scores
            s_roic = score_clip(roic, 5, 30) if roic else score_clip(roe, 5, 25)
            s_roe  = score_clip(roe, 5, 30)
            s_gm   = score_clip(gm, 15, 60)
            s_rg   = score_clip(rg, -5, 25)
            s_pio  = score_clip(pio, 3, 9)
            Q = round(0.30*s_roic + 0.20*s_roe + 0.20*s_gm + 0.15*s_rg + 0.15*s_pio, 1)

            s_ev   = score_clip(ev_eb, 5, 20, inv=True)
            s_pfcf = score_clip(pfcf_v, 5, 25, inv=True)
            s_pb   = score_clip(pb_v, 0.5, 4, inv=True)
            s_fcy  = score_clip(fcy, 1, 10)
            V = round(0.35*s_ev + 0.25*s_pfcf + 0.20*s_pb + 0.20*s_fcy, 1)

            s_m12  = score_clip(mom12, -20, 40)
            s_m6   = score_clip(mom6, -15, 30)
            s_eg   = score_clip(eg, -10, 30)
            M = round(0.50*s_m12 + 0.30*s_m6 + 0.20*s_eg, 1)

            s_alt  = score_clip(alt, 1, 4)
            s_deb  = score_clip(deb_eb, 0, 4, inv=True) if deb_eb is not None else 50
            s_cur  = score_clip(cur_r, 0.8, 2.5)
            S = round(0.40*s_alt + 0.35*s_deb + 0.25*s_cur, 1)

            s_acc  = score_clip(acc, -0.05, 0.10, inv=True) if acc is not None else 50
            s_52s  = score_clip(d52, -40, -5) if d52 is not None else 50
            F = round(0.60*s_acc + 0.40*s_52s, 1)

            score = round(0.35*Q + 0.25*V + 0.20*M + 0.15*S + 0.05*F, 1)

            vt = V > 70 and Q < 40
            if score >= 75:   verdict = "PEPITE (>=75)"
            elif score >= 55: verdict = "CANDIDAT (>=55)"
            elif score >= 40: verdict = "SURVEILLER"
            else:             verdict = "EVITER"

            if score >= 75:   sizing = "6-8% portefeuille (CONVICTION FORTE)"
            elif score >= 65: sizing = "4-5% portefeuille (CONVICTION MOYENNE)"
            elif score >= 55: sizing = "2-3% portefeuille (POSITION INITIALE)"
            else:             sizing = "0% — ne pas investir"

            alerte = None
            if rp:
                if rp < 0.80:  alerte = "FORTE HAUSSE ATTENDUE"
                elif rp < 0.92: alerte = "REVISION POSITIVE"
                elif rp > 1.15: alerte = "ATTENTION - REVISION NEGATIVE POSSIBLE"

            return {
                "Ticker": ticker, "Bourse": bourse, "name": nom, "secteur": secteur,
                "cap": cap_lab, "prix": prix_v, "devise": devise,
                "Q": Q, "V": V, "M": M, "S": S, "F": F,
                "Score": score, "Verdict": verdict,
                "roic": roic, "roe": roe, "roa": roa, "gm": gm, "pm": pm, "rg": rg, "epsg": eg,
                "pe": pe_v, "fpe": fpe_v, "pb": pb_v, "ps": ps_v, "pfcf": pfcf_v,
                "ev_ebitda": ev_eb, "fcy": fcy, "div_yield": div_y,
                "mom": mom12, "mom_6m": mom6, "mom_3m": mom3,
                "altman": alt, "debt_eb": deb_eb, "cur_r": cur_r, "pio": pio,
                "beta": beta_v, "vol_30d": vol30,
                "accruals": acc, "dist_52s": d52, "rev_proxy": rp,
                "alerte_revision": alerte, "mom_rel_sect": None,
                "liquidite": liq, "value_trap": vt, "sizing": sizing,
                "Date_Analyse": datetime.now().strftime("%Y-%m-%d"),
            }
        except Exception as e:
            if attempt < CONFIG["max_retries"] - 1:
                time.sleep(CONFIG["delai_retry"])
            else:
                print(f"ECHEC {ticker}: {str(e)[:50]}")
                return None
    return None

COLS = [
    "Ticker","Bourse","name","secteur","cap","prix","devise",
    "Q","V","M","S","F","Score","Verdict",
    "roic","roe","roa","gm","pm","rg","epsg",
    "pe","fpe","pb","ps","pfcf","ev_ebitda","fcy","div_yield",
    "mom","mom_6m","mom_3m",
    "altman","debt_eb","cur_r","pio","beta","vol_30d",
    "accruals","dist_52s","rev_proxy","alerte_revision","mom_rel_sect",
    "liquidite","value_trap","sizing","Date_Analyse",
]

def charger_csv():
    lignes = {}
    if SCREENER_CSV.exists():
        try:
            with open(SCREENER_CSV, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    lignes[row["Ticker"]] = row
        except Exception: pass
    return lignes

def is_num(s):
    try: float(s); return True
    except: return False

def sauvegarder(nouveaux):
    existants = charger_csv()
    for r in nouveaux:
        existants[r["Ticker"]] = r
    toutes = sorted(existants.values(),
                    key=lambda x: float(x.get("Score",0) or 0), reverse=True)
    with open(SCREENER_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLS, extrasaction="ignore")
        w.writeheader(); w.writerows(toutes)
    candidats = [
        {k: (float(v) if isinstance(v, str) and is_num(v) else v) for k, v in r.items()}
        for r in toutes if float(r.get("Score",0) or 0) >= CONFIG["seuil_candidat"]
    ]
    with open(CANDIDATS_JSON, "w", encoding="utf-8") as f:
        json.dump(candidats, f, ensure_ascii=False, indent=2, default=str)
    # README
    top = candidats[:10]
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    md  = ["# LA FIRME — PEA Screener Pro v3.1"]
    md += [f"_Mise a jour : {now} UTC_", ""]
    md += [f"- **Actions analysees** : {len(toutes)}"]
    md += [f"- **Candidats (>=55)** : {len(candidats)}", ""]
    md += ["| # | Ticker | Nom | Secteur | Score | Q | V | M | S |"]
    md += ["|---|--------|-----|---------|-------|---|---|---|---|"]
    for idx2, r2 in enumerate(top, 1):
        md.append(f"| {idx2} | **{r2.get('Ticker','?')}** | {r2.get('name','?')} | {r2.get('secteur','?')} | **{r2.get('Score','?')}** | {r2.get('Q','?')} | {r2.get('V','?')} | {r2.get('M','?')} | {r2.get('S','?')} |")
    md += ["", "> _Ne constitue pas un conseil en investissement._"]
    with open(README_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(md))
    return toutes, candidats

def main():
    print("=" * 55)
    print("  LA FIRME — SCREENER PRO v3.1")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 55)
    etat  = charger_etat()
    batch = selectionner_batch(etat, CONFIG["max_par_session"])
    print(f"Univers    : {len(UNIVERS_PROPRE)} actions")
    print(f"Ce batch   : {len(batch)} actions")
    print(f"Deja vues  : {len(etat)}")
    print("-" * 55)

    resultats = []
    hist_all  = {}
    nb_ok = nb_err = 0

    for i, item in enumerate(batch, 1):
        ticker, nom, secteur, bourse = item[:4]
        print(f"{i:2}/{len(batch)} {ticker:<12} {nom[:22]:<22}", end=" ")
        sys.stdout.flush()
        time.sleep(CONFIG["delai_requete"])
        r = fetch_action(ticker, nom, secteur, bourse, hist_all)
        if r:
            resultats.append(r)
            nb_ok += 1
            print(f"OK {r['Score']:5.1f} {r['Verdict']}")
        else:
            nb_err += 1
            print("ECHEC")

    # Momentum sectoriel
    for r in resultats:
        try:
            if r["Ticker"] not in hist_all or hist_all[r["Ticker"]] is None:
                continue
            hist = hist_all[r["Ticker"]]
            if len(hist) < 120: continue
            perf = hist["Close"].iloc[-1] / hist["Close"].iloc[-120] - 1
            perfs_s = []
            for t2, n2, s2, b2 in UNIVERS_PROPRE:
                if s2 == r["secteur"] and t2 != r["Ticker"] and t2 in hist_all and hist_all[t2] is not None:
                    h2 = hist_all[t2]
                    if len(h2) >= 120:
                        try: perfs_s.append(h2["Close"].iloc[-1] / h2["Close"].iloc[-120] - 1)
                        except: pass
            if len(perfs_s) >= 2:
                med = np.median(perfs_s)
                if med != 0: r["mom_rel_sect"] = round((1+perf)/(1+med), 3)
        except Exception:
            pass

    toutes, candidats = sauvegarder(resultats)
    print(f"\n{'='*55}")
    print(f"OK: {nb_ok} | ECHEC: {nb_err} | Candidats: {len(candidats)}")
    print(f"Couverture totale: {len(toutes)}/{len(UNIVERS_PROPRE)}")
    if candidats:
        print("\nTOP CANDIDATS:")
        for r in sorted(candidats, key=lambda x: float(x.get("Score",0) or 0), reverse=True)[:10]:
            print(f"  {r['Ticker']:<12} {float(r.get('Score',0) or 0):5.1f}  Q:{r.get('Q','?')} V:{r.get('V','?')}  {r.get('alerte_revision') or ''}")

if __name__ == "__main__":
    main()
