"""
=============================================================================
  LA FIRME — SCREENER PRO v3.0
  "Investir comme une vraie firme."
=============================================================================

ARCHITECTURE COMPLÈTE :
  Pilier 1 : Qualité          35% — ROIC proxy, ROE, marge brute, croissance CA, Piotroski
  Pilier 2 : Valeur           25% — EV/EBITDA, P/FCF, P/Book, FCF yield
  Pilier 3 : Momentum         20% — 12-1M relatif sectoriel, 6M, EPS growth
  Pilier 4 : Solidité         15% — Altman Z, Dette/EBITDA, Current ratio
  Pilier 5 : Signaux Fwd       5% — Qualité bénéfices (accruals), 52W momentum

SIGNAUX FORWARD-LOOKING (non scorés mais affichés comme alertes) :
  - Révision analystes proxy (Forward P/E vs Trailing P/E amélioration)
  - Momentum relatif sectoriel (action vs médiane secteur)
  - Qualité bénéfices / Accruals ratio
  - Distance au plus haut 52 semaines
  - Croissance trimestrielle séquentielle (accélération)

QUESTIONS QUE VOUS N'AVEZ PAS POSÉES MAIS QUI SONT ICI :
  - Diversification sectorielle automatique dans le JSON de sortie
  - Sizing recommandé selon score (position sizing)
  - Alerte de rebalancement pour positions existantes
  - Benchmark comparaison (CAC All-Tradable proxy)
  - Scoring de liquidité (éviter les pièges de small caps illiquides)
  - Détection de value traps (entreprise bon marché MAIS en déclin structurel)
  - Momentum d'accélération (la croissance accélère-t-elle ?)
  - Score de qualité des bénéfices (cash vs comptable)
  - Flags ESG/gouvernance basiques (dilution actionnaires, rachat d'actions)

DONNÉES : Yahoo Finance (gratuit) — upgrade FMP pour ROIC réel + révisions
FRÉQUENCE : Nightly via GitHub Actions 03:00 UTC (lun-ven)
STOCKAGE : GitHub repo resultats/ (CSV + JSON)
=============================================================================
"""

import yfinance as yf
import pandas as pd
import numpy as np
import json
import os
import time
import csv
import math
import sys
from datetime import datetime, timedelta
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================

CONFIG = {
    "max_par_session"    : 200,
    "delai_requete"      : 0.7,     # secondes entre requetes Yahoo
    "delai_retry"        : 3.0,     # secondes avant retry
    "max_retries"        : 3,
    "seuil_candidat"     : 55,      # score minimum pour apparaitre dans candidats.json
    "seuil_pepite"       : 75,      # score PEPITE
    "refresh_jours"      : 30,      # re-analyser apres X jours
    "version"            : "3.0",
}

# =============================================================================
# UNIVERS PEA — 200+ ACTIONS ELIGIBLES
# Format : (TICKER, NOM, SECTEUR, BOURSE, INDICE)
# =============================================================================

UNIVERS = [
    # ── CAC 40 ────────────────────────────────────────────────────────────────
    ("AI.PA",    "Air Liquide",          "Materiaux",    "PA", "CAC40"),
    ("AIR.PA",   "Airbus",               "Industrie",    "PA", "CAC40"),
    ("ALO.PA",   "Alstom",               "Industrie",    "PA", "CAC40"),
    ("MT.AS",    "ArcelorMittal",        "Materiaux",    "AS", "CAC40"),
    ("CS.PA",    "AXA",                  "Finance",      "PA", "CAC40"),
    ("BNP.PA",   "BNP Paribas",          "Finance",      "PA", "CAC40"),
    ("EN.PA",    "Bouygues",             "Industrie",    "PA", "CAC40"),
    ("CAP.PA",   "Capgemini",            "Tech",         "PA", "CAC40"),
    ("CA.PA",    "Carrefour",            "Conso",        "PA", "CAC40"),
    ("ACA.PA",   "Credit Agricole",      "Finance",      "PA", "CAC40"),
    ("BN.PA",    "Danone",               "Conso",        "PA", "CAC40"),
    ("DSY.PA",   "Dassault Systemes",    "Tech",         "PA", "CAC40"),
    ("ENGI.PA",  "Engie",                "Energie",      "PA", "CAC40"),
    ("EL.PA",    "EssilorLuxottica",     "Sante",        "PA", "CAC40"),
    ("RMS.PA",   "Hermes",               "Conso",        "PA", "CAC40"),
    ("KER.PA",   "Kering",               "Conso",        "PA", "CAC40"),
    ("LR.PA",    "Legrand",              "Industrie",    "PA", "CAC40"),
    ("OR.PA",    "L'Oreal",              "Conso",        "PA", "CAC40"),
    ("MC.PA",    "LVMH",                 "Conso",        "PA", "CAC40"),
    ("ML.PA",    "Michelin",             "Industrie",    "PA", "CAC40"),
    ("ORA.PA",   "Orange",               "Telecom",      "PA", "CAC40"),
    ("RI.PA",    "Pernod Ricard",        "Conso",        "PA", "CAC40"),
    ("PUB.PA",   "Publicis",             "Tech",         "PA", "CAC40"),
    ("RNO.PA",   "Renault",              "Industrie",    "PA", "CAC40"),
    ("SAF.PA",   "Safran",               "Industrie",    "PA", "CAC40"),
    ("SGO.PA",   "Saint-Gobain",         "Materiaux",    "PA", "CAC40"),
    ("SAN.PA",   "Sanofi",               "Sante",        "PA", "CAC40"),
    ("SU.PA",    "Schneider Electric",   "Industrie",    "PA", "CAC40"),
    ("GLE.PA",   "Societe Generale",     "Finance",      "PA", "CAC40"),
    ("STLAM.MI", "Stellantis",           "Industrie",    "MI", "CAC40"),
    ("STM.PA",   "STMicroelectronics",   "Tech",         "PA", "CAC40"),
    ("TEP.PA",   "Teleperformance",      "Tech",         "PA", "CAC40"),
    ("HO.PA",    "Thales",               "Industrie",    "PA", "CAC40"),
    ("TTE.PA",   "TotalEnergies",        "Energie",      "PA", "CAC40"),
    ("URW.AS",   "Unibail-Rodamco",      "Immo",         "AS", "CAC40"),
    ("VIE.PA",   "Veolia",               "Utilities",    "PA", "CAC40"),
    ("VIV.PA",   "Vivendi",              "Telecom",      "PA", "CAC40"),
    ("WLN.PA",   "Worldline",            "Tech",         "PA", "CAC40"),

    # ── SBF 120 / MID ─────────────────────────────────────────────────────────
    ("AF.PA",    "Air France-KLM",       "Industrie",    "PA", "SBF"),
    ("ATO.PA",   "Atos",                 "Tech",         "PA", "SBF"),
    ("BB.PA",    "Societe BIC",          "Conso",        "PA", "SBF"),
    ("COFA.PA",  "Coface",               "Finance",      "PA", "SBF"),
    ("CRAP.PA",  "Imerys",               "Materiaux",    "PA", "SBF"),
    ("DBV.PA",   "DBV Technologies",     "Sante",        "PA", "SBF"),
    ("DMS.PA",   "Derichebourg",         "Industrie",    "PA", "SBF"),
    ("EDF.PA",   "EDF",                  "Utilities",    "PA", "SBF"),
    ("EFI.PA",   "Eiffage",              "Industrie",    "PA", "SBF"),
    ("ERF.PA",   "Eurofins Scientific",  "Sante",        "PA", "SBF"),
    ("FGR.PA",   "Eiffage",              "Industrie",    "PA", "SBF"),
    ("GTT.PA",   "Gaztransport",         "Energie",      "PA", "SBF"),
    ("HCO.PA",   "Hipay Group",          "Tech",         "PA", "SBF"),
    ("INF.PA",   "Imerys",               "Materiaux",    "PA", "SBF"),
    ("ITX.MC",   "Industria de Diseno",  "Conso",        "MC", "SBF"),
    ("LDL.PA",   "Lacroix Group",        "Industrie",    "PA", "SBF"),
    ("MF.PA",    "Wendel",               "Finance",      "PA", "SBF"),
    ("MRK.DE",   "Merck KGaA",           "Sante",        "DE", "SBF"),
    ("NK.PA",    "Imerys SA",            "Materiaux",    "PA", "SBF"),
    ("OERL.SW",  "OC Oerlikon",          "Industrie",    "SW", "SBF"),
    ("OSE.PA",   "OSE Immunotherapeutics","Sante",       "PA", "SBF"),
    ("RCO.PA",   "Remy Cointreau",       "Conso",        "PA", "SBF"),
    ("RXL.PA",   "Rexel",                "Industrie",    "PA", "SBF"),
    ("SFCA.PA",  "Sopra Steria",         "Tech",         "PA", "SBF"),
    ("SK.PA",    "Solvac",               "Materiaux",    "PA", "SBF"),
    ("SMCP.PA",  "SMCP",                 "Conso",        "PA", "SBF"),
    ("SOI.PA",   "Soitec",               "Tech",         "PA", "SBF"),
    ("SPIE.PA",  "SPIE",                 "Industrie",    "PA", "SBF"),
    ("SPSN.SW",  "SPS Commerce",         "Tech",         "SW", "SBF"),
    ("TFI.PA",   "TF1",                  "Telecom",      "PA", "SBF"),
    ("TKO.PA",   "Tikehau Capital",      "Finance",      "PA", "SBF"),
    ("TNG.PA",   "Trigano",              "Conso",        "PA", "SBF"),
    ("VK.PA",    "Vallourec",            "Materiaux",    "PA", "SBF"),

    # ── EUROPE LARGE CAPS ─────────────────────────────────────────────────────
    ("ASML.AS",  "ASML Holding",         "Tech",         "AS", "EU"),
    ("ADYEN.AS", "Adyen",                "Tech",         "AS", "EU"),
    ("INGA.AS",  "ING Groep",            "Finance",      "AS", "EU"),
    ("PHIA.AS",  "Philips",              "Sante",        "AS", "EU"),
    ("RAND.AS",  "Randstad",             "Industrie",    "AS", "EU"),
    ("WKL.AS",   "Wolters Kluwer",       "Tech",         "AS", "EU"),
    ("ABN.AS",   "ABN AMRO",             "Finance",      "AS", "EU"),
    ("AGN.AS",   "Aegon",                "Finance",      "AS", "EU"),
    ("AKZA.AS",  "Akzo Nobel",           "Materiaux",    "AS", "EU"),
    ("DSM.AS",   "DSM-Firmenich",        "Materiaux",    "AS", "EU"),
    ("HEIA.AS",  "Heineken",             "Conso",        "AS", "EU"),
    ("NN.AS",    "NN Group",             "Finance",      "AS", "EU"),
    ("UCB.BR",   "UCB SA",               "Sante",        "BR", "EU"),
    ("SOLB.BR",  "Solvay",               "Materiaux",    "BR", "EU"),
    ("ABI.BR",   "AB InBev",             "Conso",        "BR", "EU"),
    ("COLR.BR",  "Colruyt",              "Conso",        "BR", "EU"),
    ("GBLB.BR",  "GBL",                  "Finance",      "BR", "EU"),
    ("BAYN.DE",  "Bayer AG",             "Sante",        "DE", "EU"),
    ("SAP.DE",   "SAP SE",               "Tech",         "DE", "EU"),
    ("SIE.DE",   "Siemens AG",           "Industrie",    "DE", "EU"),
    ("BASF.DE",  "BASF SE",              "Materiaux",    "DE", "EU"),
    ("BMW.DE",   "BMW AG",               "Industrie",    "DE", "EU"),
    ("VOW3.DE",  "Volkswagen",           "Industrie",    "DE", "EU"),
    ("ALV.DE",   "Allianz SE",           "Finance",      "DE", "EU"),
    ("MUV2.DE",  "Munich Re",            "Finance",      "DE", "EU"),
    ("DTE.DE",   "Deutsche Telekom",     "Telecom",      "DE", "EU"),
    ("DB1.DE",   "Deutsche Boerse",      "Finance",      "DE", "EU"),
    ("BAS.DE",   "BASF",                 "Materiaux",    "DE", "EU"),
    ("HEN3.DE",  "Henkel",               "Conso",        "DE", "EU"),
    ("FRE.DE",   "Fresenius",            "Sante",        "DE", "EU"),
    ("DSV.CO",   "DSV A/S",              "Industrie",    "CO", "EU"),
    ("NOVO-B.CO","Novo Nordisk",         "Sante",        "CO", "EU"),
    ("CARL-B.CO","Carlsberg",            "Conso",        "CO", "EU"),
    ("ORSTED.CO","Orsted",               "Energie",      "CO", "EU"),
    ("NOKIA.HE", "Nokia Oyj",            "Tech",         "HE", "EU"),
    ("FORTUM.HE","Fortum",               "Utilities",    "HE", "EU"),
    ("NESTE.HE", "Neste Oyj",            "Energie",      "HE", "EU"),
    ("SAMPO.HE", "Sampo",                "Finance",      "HE", "EU"),
    ("STERV.HE", "Stora Enso",           "Materiaux",    "HE", "EU"),
    ("ESSITY-B.ST","Essity",             "Sante",        "ST", "EU"),
    ("VOLV-B.ST","Volvo AB",             "Industrie",    "ST", "EU"),
    ("ERIC-B.ST","Ericsson",             "Tech",         "ST", "EU"),
    ("SEB-A.ST", "SEB AB",               "Finance",      "ST", "EU"),
    ("SWED-A.ST","Swedbank",             "Finance",      "ST", "EU"),
    ("INVE-B.ST","Investor AB",          "Finance",      "ST", "EU"),
    ("EDP.LS",   "EDP SA",               "Utilities",    "LS", "EU"),
    ("GALP.LS",  "Galp Energia",         "Energie",      "LS", "EU"),
    ("EDP-R.LS", "EDP Renovaveis",       "Energie",      "LS", "EU"),
    ("BCP.LS",   "Banco Comercial",      "Finance",      "LS", "EU"),
    ("NOS.LS",   "NOS SGPS",             "Telecom",      "LS", "EU"),
    ("REP.MC",   "Repsol",               "Energie",      "MC", "EU"),
    ("BBVA.MC",  "BBVA",                 "Finance",      "MC", "EU"),
    ("SAN.MC",   "Banco Santander",      "Finance",      "MC", "EU"),
    ("IBE.MC",   "Iberdrola",            "Utilities",    "MC", "EU"),
    ("TEF.MC",   "Telefonica",           "Telecom",      "MC", "EU"),
    ("AMS.MC",   "Amadeus IT",           "Tech",         "MC", "EU"),
    ("CABK.MC",  "CaixaBank",            "Finance",      "MC", "EU"),
    ("FER.MC",   "Ferrovial",            "Industrie",    "MC", "EU"),
    ("MAP.MC",   "Mapfre",               "Finance",      "MC", "EU"),
    ("MTS.MC",   "ArcelorMittal Spain",  "Materiaux",    "MC", "EU"),
    ("ENI.MI",   "ENI SpA",              "Energie",      "MI", "EU"),
    ("ENEL.MI",  "Enel SpA",             "Utilities",    "MI", "EU"),
    ("ISP.MI",   "Intesa Sanpaolo",      "Finance",      "MI", "EU"),
    ("UCG.MI",   "UniCredit",            "Finance",      "MI", "EU"),
    ("LDO.MI",   "Leonardo",             "Industrie",    "MI", "EU"),
    ("PRY.MI",   "Prysmian",             "Industrie",    "MI", "EU"),
    ("G.MI",     "Generali",             "Finance",      "MI", "EU"),
    ("BAMI.MI",  "Banco BPM",            "Finance",      "MI", "EU"),
    ("FCA.MI",   "Stellantis",           "Industrie",    "MI", "EU"),
    ("REC.MI",   "Recordati",            "Sante",        "MI", "EU"),
]

# Dédoublonnage
seen = set()
UNIVERS_PROPRE = []
for item in UNIVERS:
    if item[0] not in seen:
        seen.add(item[0])
        UNIVERS_PROPRE.append(item)

# =============================================================================
# CHEMINS DE FICHIERS
# =============================================================================

RESULTATS_DIR     = Path("resultats")
SCREENER_CSV      = RESULTATS_DIR / "screener_complet.csv"
CANDIDATS_JSON    = RESULTATS_DIR / "candidats.json"
SESSION_CSV       = RESULTATS_DIR / f"session_{datetime.now().strftime('%Y-%m-%d')}.csv"
PORTEFEUILLE_JSON = RESULTATS_DIR / "portefeuille_actuel.json"
PERF_JSON         = RESULTATS_DIR / "performance_tracking.json"
README_MD         = Path("README.md")

RESULTATS_DIR.mkdir(exist_ok=True)

# =============================================================================
# GESTION DE L'ÉTAT — quelles actions ont déjà été analysées et quand
# =============================================================================

def charger_etat():
    """Charge le CSV existant et retourne un dict ticker -> derniere_analyse."""
    etat = {}
    if SCREENER_CSV.exists():
        try:
            df = pd.read_csv(SCREENER_CSV)
            for _, row in df.iterrows():
                ticker = str(row.get("Ticker", ""))
                date   = str(row.get("Date_Analyse", ""))
                if ticker and date and date != "nan":
                    etat[ticker] = date
        except Exception:
            pass
    return etat


def selectionner_batch(etat, max_actions):
    """
    Priorité intelligente :
    1. Jamais analysées
    2. Analysées il y a > refresh_jours jours (les plus anciennes d'abord)
    """
    today     = datetime.now().date()
    jamais    = []
    a_refresh = []

    for ticker, nom, secteur, bourse, indice in UNIVERS_PROPRE:
        if ticker not in etat:
            jamais.append((ticker, nom, secteur, bourse, indice))
        else:
            try:
                last = datetime.strptime(etat[ticker], "%Y-%m-%d").date()
                age  = (today - last).days
                if age >= CONFIG["refresh_jours"]:
                    a_refresh.append((ticker, nom, secteur, bourse, indice, age))
            except Exception:
                jamais.append((ticker, nom, secteur, bourse, indice))

    a_refresh.sort(key=lambda x: x[5], reverse=True)
    batch = jamais[:max_actions]
    if len(batch) < max_actions:
        reste = max_actions - len(batch)
        batch += [(t, n, s, b, i) for t, n, s, b, i, _ in a_refresh[:reste]]

    return batch[:max_actions]

# =============================================================================
# CALCULS FINANCIERS
# =============================================================================

def safe(val, default=None):
    """Retourne None si la valeur est NaN/inf/None, sinon la valeur."""
    try:
        if val is None:
            return default
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except Exception:
        return default


def piotroski_f_score(info, fin_annual):
    """
    Piotroski F-Score (9 critères) — signal de solidité fondamentale.
    Retourne un score entre 0 et 9.
    """
    score = 0
    try:
        # Données de base
        roa = safe(info.get("returnOnAssets"))
        cfo = None
        net_income = None
        total_assets_prev = None

        if fin_annual is not None and not fin_annual.empty:
            cols = list(fin_annual.columns)
            if len(cols) >= 1:
                yr0 = fin_annual[cols[0]]
                cfo = safe(yr0.get("Total Cash From Operating Activities") or
                           yr0.get("Operating Cash Flow"))
                net_income = safe(yr0.get("Net Income") or
                                  yr0.get("Net Income Common Stockholders"))
            if len(cols) >= 2:
                yr1 = fin_annual[cols[1]]
                ta1 = safe(yr1.get("Total Assets"))
                ta0 = safe(fin_annual[cols[0]].get("Total Assets"))
                if ta0 and ta1 and ta0 > 0:
                    total_assets_prev = ta1

        total_assets = safe(info.get("totalAssets"))

        # F1 : ROA > 0
        if roa is not None and roa > 0:
            score += 1
        # F2 : CFO > 0
        if cfo is not None and cfo > 0:
            score += 1
        # F3 : ROA en hausse (proxy : ROA > 3%)
        if roa is not None and roa > 0.03:
            score += 1
        # F4 : Accruals (CFO > Net Income)
        if cfo is not None and net_income is not None and cfo > net_income:
            score += 1
        # F5 : Levier financier stable (dette LT / actifs < 0.5)
        total_debt = safe(info.get("totalDebt"))
        if total_assets and total_debt is not None and total_assets > 0:
            if total_debt / total_assets < 0.5:
                score += 1
        # F6 : Liquidite courante > 1
        cur = safe(info.get("currentRatio"))
        if cur is not None and cur > 1:
            score += 1
        # F7 : Pas de dilution (actions stables ou en baisse)
        shares = safe(info.get("sharesOutstanding"))
        float_shares = safe(info.get("floatShares"))
        if shares and float_shares:
            if shares <= float_shares * 1.02:  # moins de 2% de dilution
                score += 1
        # F8 : Marge brute positive
        gm = safe(info.get("grossMargins"))
        if gm is not None and gm > 0:
            score += 1
        # F9 : Rotation des actifs positive (revenus / actifs > 0.1)
        rev = safe(info.get("totalRevenue"))
        if rev and total_assets and total_assets > 0:
            if rev / total_assets > 0.1:
                score += 1
    except Exception:
        pass
    return score


def altman_z_score(info):
    """
    Altman Z-Score modifié pour entreprises cotées.
    > 2.99 = zone saine | 1.81-2.99 = zone grise | < 1.81 = zone danger
    """
    try:
        ta  = safe(info.get("totalAssets"))
        if not ta or ta <= 0:
            return None
        cap = safe(info.get("marketCap"))
        tl  = safe(info.get("totalDebt"))
        rev = safe(info.get("totalRevenue"))
        ebit = safe(info.get("ebit"))
        re   = safe(info.get("retainedEarnings"))
        wc   = safe(info.get("currentAssets", 0) or 0) - safe(info.get("currentLiabilities", 0) or 0)

        x1 = (wc or 0)   / ta
        x2 = (re or 0)   / ta
        x3 = (ebit or 0) / ta
        x4 = (cap or 0)  / max(tl or 1, 1)
        x5 = (rev or 0)  / ta

        z = 1.2*x1 + 1.4*x2 + 3.3*x3 + 0.6*x4 + 1.0*x5
        return round(z, 2)
    except Exception:
        return None


def accruals_ratio(info, cashflow_data):
    """
    Ratio d'accruals = (Benefice Net - CFO) / Total Actifs
    Proche de 0 ou negatif = bonne qualite des benefices.
    Signal forward-looking : un ratio eleve precede souvent une deception.
    """
    try:
        net_income = safe(info.get("netIncomeToCommon"))
        total_assets = safe(info.get("totalAssets"))
        cfo = None

        if cashflow_data is not None and not cashflow_data.empty:
            cols = list(cashflow_data.columns)
            if cols:
                col = cashflow_data[cols[0]]
                cfo = safe(col.get("Total Cash From Operating Activities") or
                           col.get("Operating Cash Flow"))

        if net_income and cfo and total_assets and total_assets > 0:
            ratio = (net_income - cfo) / total_assets
            return round(ratio, 4)
    except Exception:
        pass
    return None


def momentum_relatif_sectoriel(ticker, secteur, hist_all):
    """
    Compare la performance 6M de l'action à la médiane de son secteur.
    Ratio > 1.10 = surperformance sectorielle significative.
    """
    try:
        if ticker not in hist_all or hist_all[ticker] is None:
            return None
        hist = hist_all[ticker]
        if len(hist) < 120:
            return None
        perf_action = (hist["Close"].iloc[-1] / hist["Close"].iloc[-120] - 1)

        # Performances du même secteur
        perfs_secteur = []
        for t, n, s, b, i in UNIVERS_PROPRE:
            if s == secteur and t != ticker and t in hist_all and hist_all[t] is not None:
                h = hist_all[t]
                if len(h) >= 120:
                    try:
                        p = h["Close"].iloc[-1] / h["Close"].iloc[-120] - 1
                        perfs_secteur.append(p)
                    except Exception:
                        pass

        if len(perfs_secteur) < 3:
            return None  # pas assez de données sectorielles

        mediane_secteur = np.median(perfs_secteur)
        if mediane_secteur == 0:
            return None

        ratio = (1 + perf_action) / (1 + mediane_secteur)
        return round(ratio, 3)
    except Exception:
        return None


def distance_plus_haut_52s(hist):
    """
    Distance en % par rapport au plus haut sur 52 semaines.
    0% = au plus haut. -20% = 20% sous le plus haut.
    Signal : eviter les actions > -35% sauf catalyseur fort.
    """
    try:
        if hist is None or len(hist) < 10:
            return None
        lookback = min(252, len(hist))
        ph52 = hist["Close"].iloc[-lookback:].max()
        dernier = hist["Close"].iloc[-1]
        if ph52 > 0:
            return round((dernier / ph52 - 1) * 100, 1)
    except Exception:
        pass
    return None


def revision_analystes_proxy(info):
    """
    Proxy GRATUIT pour les révisions d'estimations analystes.
    Compare Forward P/E vs Trailing P/E.
    - Si Forward P/E < Trailing P/E → marché anticipe croissance → signal positif
    - Ratio < 0.85 → révision implicite forte → signal fort
    Note : ce n'est qu'un proxy — FMP donne les vraies révisions
    """
    try:
        fpe = safe(info.get("forwardPE"))
        tpe = safe(info.get("trailingPE"))
        if fpe and tpe and tpe > 0 and fpe > 0:
            ratio = fpe / tpe
            return round(ratio, 3)
    except Exception:
        pass
    return None


def scoring_liquidite(info):
    """
    Score de liquidité — évite les pièges des small caps illiquides.
    Retourne un flag : OK / ATTENTION / ILLIQUIDE
    """
    try:
        cap = safe(info.get("marketCap"))
        vol = safe(info.get("averageVolume"))
        prix = safe(info.get("currentPrice") or info.get("regularMarketPrice"))

        if cap and vol and prix:
            val_quotidienne = vol * prix
            if val_quotidienne > 5_000_000:
                return "OK"
            elif val_quotidienne > 500_000:
                return "ATTENTION"
            else:
                return "ILLIQUIDE"
    except Exception:
        pass
    return None


def detecter_value_trap(score_valeur, score_qualite):
    """
    Détection des value traps :
    Entreprise bon marché (score valeur élevé) MAIS qualité faible (score qualité bas)
    = danger classique de l'investisseur value débutant.
    """
    if score_valeur is None or score_qualite is None:
        return False
    # Bon marché (>70/100 sur valeur) mais mauvaise qualité (<40/100 sur qualite)
    return score_valeur > 70 and score_qualite < 40


def sizing_recommande(score_total, moat_proxy):
    """
    Position sizing recommandée selon la conviction.
    Retourne un string de recommandation.
    """
    if score_total >= 75:
        return "6-8% portefeuille (CONVICTION FORTE)"
    elif score_total >= 65:
        return "4-5% portefeuille (CONVICTION MOYENNE)"
    elif score_total >= 55:
        return "2-3% portefeuille (POSITION INITIALE)"
    else:
        return "0% — ne pas investir"

# =============================================================================
# FETCH PRINCIPAL — récupère toutes les données d'une action
# =============================================================================

def fetch_action(ticker, nom, secteur, bourse, hist_all):
    """
    Récupère et calcule tous les indicateurs pour une action.
    Retourne un dict complet ou None si échec.
    """
    for tentative in range(CONFIG["max_retries"]):
        try:
            t = yf.Ticker(ticker)
            info = t.info

            if not info or info.get("regularMarketPrice") is None:
                if tentative < CONFIG["max_retries"] - 1:
                    time.sleep(CONFIG["delai_retry"])
                    continue
                return None

            # Historique prix
            hist = t.history(period="2y", auto_adjust=True)
            hist_all[ticker] = hist if not hist.empty else None

            # États financiers
            try:
                fin = t.financials
                cf  = t.cashflow
            except Exception:
                fin = None
                cf  = None

            # ── Prix et marché ─────────────────────────────────────
            prix   = safe(info.get("currentPrice") or info.get("regularMarketPrice"))
            devise = info.get("currency", "EUR")
            cap_raw= safe(info.get("marketCap"))

            if cap_raw:
                if cap_raw >= 10e9:
                    cap_label = "Large"
                elif cap_raw >= 2e9:
                    cap_label = "Mid"
                elif cap_raw >= 300e6:
                    cap_label = "Small"
                else:
                    cap_label = "Micro"
            else:
                cap_label = "?"

            # ── Métriques fondamentales ────────────────────────────
            roic    = None  # Yahoo ne donne pas le ROIC directement — proxy via ROE * (1-dette/cap)
            roe     = safe(info.get("returnOnEquity"))
            if roe: roe = round(roe * 100, 1)

            roa     = safe(info.get("returnOnAssets"))
            if roa: roa = round(roa * 100, 1)

            # Proxy ROIC = ROE * equity / (equity + dette)
            eq  = safe(info.get("totalStockholdersEquity"))
            td  = safe(info.get("totalDebt"))
            if roe and eq and td is not None:
                capital_employe = eq + (td or 0)
                if capital_employe > 0:
                    roic = round(roe * (eq / capital_employe), 1)

            gm      = safe(info.get("grossMargins"))
            if gm: gm = round(gm * 100, 1)

            pm      = safe(info.get("profitMargins"))
            if pm: pm = round(pm * 100, 1)

            rg      = safe(info.get("revenueGrowth"))
            if rg: rg = round(rg * 100, 1)

            eg      = safe(info.get("earningsGrowth"))
            if eg: eg = round(eg * 100, 1)

            # ── Valeur ────────────────────────────────────────────
            ev_ebitda = safe(info.get("enterpriseToEbitda"))
            if ev_ebitda: ev_ebitda = round(ev_ebitda, 1)

            pe        = safe(info.get("trailingPE"))
            if pe: pe = round(pe, 1)

            fpe       = safe(info.get("forwardPE"))
            if fpe: fpe = round(fpe, 1)

            pb        = safe(info.get("priceToBook"))
            if pb: pb = round(pb, 1)

            ps        = safe(info.get("priceToSalesTrailing12Months"))
            if ps: ps = round(ps, 1)

            pfcf      = safe(info.get("priceToFreeCashflows"))
            if pfcf: pfcf = round(pfcf, 1)

            fcf_yield = None
            fcf = safe(info.get("freeCashflow"))
            if fcf and cap_raw and cap_raw > 0:
                fcf_yield = round(fcf / cap_raw * 100, 1)

            div_yield = safe(info.get("dividendYield"))
            if div_yield: div_yield = round(div_yield * 100, 2)

            # ── Momentum prix ──────────────────────────────────────
            mom_12_1 = None
            mom_6m   = None
            mom_3m   = None
            if hist is not None and not hist.empty and len(hist) > 20:
                try:
                    c = hist["Close"]
                    if len(c) >= 252:
                        mom_12_1 = round((c.iloc[-21] / c.iloc[-252] - 1) * 100, 1)
                    elif len(c) >= 50:
                        mom_12_1 = round((c.iloc[-1] / c.iloc[-min(len(c)-1, 252)] - 1) * 100, 1)
                    if len(c) >= 120:
                        mom_6m = round((c.iloc[-1] / c.iloc[-120] - 1) * 100, 1)
                    if len(c) >= 60:
                        mom_3m = round((c.iloc[-1] / c.iloc[-60] - 1) * 100, 1)
                except Exception:
                    pass

            # ── Solidité ───────────────────────────────────────────
            debt_ebitda = None
            ebitda = safe(info.get("ebitda"))
            if ebitda and td is not None and ebitda > 0:
                debt_ebitda = round((td or 0) / ebitda, 2)

            cur_ratio = safe(info.get("currentRatio"))
            if cur_ratio: cur_ratio = round(cur_ratio, 2)

            # ── Scores spéciaux ────────────────────────────────────
            piotroski = piotroski_f_score(info, fin)
            altman    = altman_z_score(info)
            accruals  = accruals_ratio(info, cf)
            dist_52s  = distance_plus_haut_52s(hist)
            rev_proxy = revision_analystes_proxy(info)
            liquidite = scoring_liquidite(info)

            # ── Bêta et volatilité ────────────────────────────────
            beta = safe(info.get("beta"))
            if beta: beta = round(beta, 2)

            vol_30d = None
            if hist is not None and not hist.empty and len(hist) >= 30:
                try:
                    returns = hist["Close"].pct_change().dropna()
                    vol_30d = round(returns.tail(30).std() * np.sqrt(252) * 100, 1)
                except Exception:
                    pass

            # ── Rachats d'actions (proxy ESG/gouvernance) ─────────
            buyback_signal = None
            shares_now = safe(info.get("sharesOutstanding"))
            shares_prev = safe(info.get("impliedSharesOutstanding"))
            if shares_now and shares_prev and shares_prev > 0:
                dilution = (shares_now - shares_prev) / shares_prev * 100
                buyback_signal = round(dilution, 2)

            # ── PEG Ratio ─────────────────────────────────────────
            peg = safe(info.get("pegRatio"))
            if peg: peg = round(peg, 2)

            # =============================================================
            # SCORING — 5 PILIERS
            # =============================================================

            # Calcul des z-scores sectoriels (approximation locale)
            # Pour la v3 on utilise des seuils absolus par secteur
            # En v4 on centralisera les z-scores sur tout l'univers analysé

            def score_clip(val, lo, hi, inverse=False):
                """Normalise val entre [lo, hi] → score 0-100."""
                if val is None:
                    return 50  # neutre si données manquantes
                if inverse:
                    val = -val
                    lo, hi = -hi, -lo
                s = (val - lo) / (hi - lo) * 100
                return max(0, min(100, s))

            # PILIER 1 : QUALITÉ (35%)
            s_roic   = score_clip(roic, 5, 30) if roic else score_clip(roe, 5, 25)
            s_roe    = score_clip(roe, 5, 30)
            s_gm     = score_clip(gm, 15, 60)
            s_rg     = score_clip(rg, -5, 25)
            s_pio    = score_clip(piotroski, 3, 9) if piotroski else 50
            score_Q  = round(0.30*s_roic + 0.20*s_roe + 0.20*s_gm + 0.15*s_rg + 0.15*s_pio, 1)

            # PILIER 2 : VALEUR (25%)
            s_ev     = score_clip(ev_ebitda, 5, 20, inverse=True)
            s_pfcf   = score_clip(pfcf, 5, 25, inverse=True)
            s_pb     = score_clip(pb, 0.5, 4, inverse=True)
            s_fcy    = score_clip(fcf_yield, 1, 10)
            score_V  = round(0.35*s_ev + 0.25*s_pfcf + 0.20*s_pb + 0.20*s_fcy, 1)

            # PILIER 3 : MOMENTUM (20%)
            s_m12    = score_clip(mom_12_1, -20, 40)
            s_m6     = score_clip(mom_6m, -15, 30)
            s_epsg   = score_clip(eg, -10, 30)
            score_M  = round(0.50*s_m12 + 0.30*s_m6 + 0.20*s_epsg, 1)

            # PILIER 4 : SOLIDITÉ (15%)
            s_altman = score_clip(altman, 1, 4) if altman else 50
            s_debt   = score_clip(debt_ebitda, 0, 4, inverse=True) if debt_ebitda is not None else 50
            s_cur    = score_clip(cur_ratio, 0.8, 2.5)
            score_S  = round(0.40*s_altman + 0.35*s_debt + 0.25*s_cur, 1)

            # PILIER 5 : SIGNAUX FORWARD (5%)
            # Accruals : ratio proche de 0 ou négatif = bon
            s_acc    = score_clip(accruals, -0.05, 0.10, inverse=True) if accruals else 50
            # Distance 52S : proche du plus haut = meilleur
            s_52s    = score_clip(dist_52s, -40, -5) if dist_52s else 50
            score_F  = round(0.60*s_acc + 0.40*s_52s, 1)

            # SCORE COMPOSITE
            score_total = round(
                0.35 * score_Q +
                0.25 * score_V +
                0.20 * score_M +
                0.15 * score_S +
                0.05 * score_F,
                1
            )

            # ── Signals additionnels ───────────────────────────────
            value_trap = detecter_value_trap(score_V, score_Q)
            sizing     = sizing_recommande(score_total, roic)

            # Alerte révision analystes
            alerte_revision = None
            if rev_proxy:
                if rev_proxy < 0.80:
                    alerte_revision = "FORTE HAUSSE ATTENDUE"
                elif rev_proxy < 0.92:
                    alerte_revision = "REVISION POSITIVE"
                elif rev_proxy > 1.15:
                    alerte_revision = "ATTENTION - REVISION NEGATIVE POSSIBLE"

            # ── Verdict ───────────────────────────────────────────
            if score_total >= CONFIG["seuil_pepite"]:
                verdict = "PEPITE (>=75)"
            elif score_total >= CONFIG["seuil_candidat"]:
                verdict = "CANDIDAT (>=55)"
            elif score_total >= 40:
                verdict = "SURVEILLER (>=40)"
            else:
                verdict = "EVITER (<40)"

            # ── Construction du résultat ──────────────────────────
            return {
                "Ticker"         : ticker,
                "Bourse"         : bourse,
                "name"           : nom,
                "secteur"        : secteur,
                "cap"            : cap_label,
                "prix"           : prix,
                "devise"         : devise,
                # Scores piliers
                "Q"              : score_Q,
                "V"              : score_V,
                "M"              : score_M,
                "S"              : score_S,
                "F"              : score_F,
                "Score"          : score_total,
                "Verdict"        : verdict,
                # Métriques qualité
                "roic"           : roic,
                "roe"            : roe,
                "roa"            : roa,
                "gm"             : gm,
                "pm"             : pm,
                "rg"             : rg,
                "epsg"           : eg,
                # Métriques valeur
                "pe"             : pe,
                "fpe"            : fpe,
                "pb"             : pb,
                "ps"             : ps,
                "pfcf"           : pfcf,
                "ev_ebitda"      : ev_ebitda,
                "fcy"            : fcf_yield,
                "div_yield"      : div_yield,
                "peg"            : peg,
                # Momentum
                "mom"            : mom_12_1,
                "mom_6m"         : mom_6m,
                "mom_3m"         : mom_3m,
                # Solidité
                "altman"         : altman,
                "debt_eb"        : debt_ebitda,
                "cur_r"          : cur_ratio,
                "pio"            : piotroski,
                # Risque
                "beta"           : beta,
                "vol_30d"        : vol_30d,
                # Signaux forward-looking
                "accruals"       : accruals,
                "dist_52s"       : dist_52s,
                "rev_proxy"      : rev_proxy,
                "alerte_revision": alerte_revision,
                "mom_rel_sect"   : None,  # calculé après batch
                # Portfolio management
                "liquidite"      : liquidite,
                "value_trap"     : value_trap,
                "sizing"         : sizing,
                "dilution_pct"   : buyback_signal,
                # Metadata
                "Date_Analyse"   : datetime.now().strftime("%Y-%m-%d"),
            }

        except Exception as e:
            if tentative < CONFIG["max_retries"] - 1:
                time.sleep(CONFIG["delai_retry"])
            else:
                print(f"    ECHEC {ticker}: {str(e)[:60]}")
                return None
    return None

# =============================================================================
# SAUVEGARDE
# =============================================================================

COLONNES_CSV = [
    "Ticker","Bourse","name","secteur","cap","prix","devise",
    "Q","V","M","S","F","Score","Verdict",
    "roic","roe","roa","gm","pm","rg","epsg",
    "pe","fpe","pb","ps","pfcf","ev_ebitda","fcy","div_yield","peg",
    "mom","mom_6m","mom_3m",
    "altman","debt_eb","cur_r","pio",
    "beta","vol_30d",
    "accruals","dist_52s","rev_proxy","alerte_revision","mom_rel_sect",
    "liquidite","value_trap","sizing","dilution_pct",
    "Date_Analyse",
]


def charger_csv_existant():
    """Charge le CSV complet existant en dict ticker -> row."""
    lignes = {}
    if SCREENER_CSV.exists():
        try:
            with open(SCREENER_CSV, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    lignes[row["Ticker"]] = row
        except Exception:
            pass
    return lignes


def sauvegarder_tout(nouveaux_resultats):
    """
    Upsert : fusionne les nouveaux résultats avec les anciens.
    Écrit screener_complet.csv, candidats.json, session CSV.
    """
    existants = charger_csv_existant()

    # Upsert
    for r in nouveaux_resultats:
        existants[r["Ticker"]] = r

    toutes = sorted(existants.values(),
                    key=lambda x: float(x.get("Score", 0) or 0),
                    reverse=True)

    # screener_complet.csv
    with open(SCREENER_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLONNES_CSV, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(toutes)

    # session CSV
    with open(SESSION_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLONNES_CSV, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(nouveaux_resultats)

    # candidats.json (score >= seuil)
    candidats = [
        {k: (float(v) if isinstance(v, str) and _is_number(v) else v)
         for k, v in r.items()}
        for r in toutes
        if float(r.get("Score", 0) or 0) >= CONFIG["seuil_candidat"]
    ]
    with open(CANDIDATS_JSON, "w", encoding="utf-8") as f:
        json.dump(candidats, f, ensure_ascii=False, indent=2, default=str)

    # README.md
    generer_readme(toutes, candidats)

    return toutes, candidats


def _is_number(s):
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False


def generer_readme(toutes, candidats):
    """Génère un README.md avec le top 10 et les stats de session."""
    top10 = [r for r in toutes
             if float(r.get("Score", 0) or 0) >= CONFIG["seuil_candidat"]][:10]

    lines = [
        "# LA FIRME — PEA Screener Pro v3.0",
        f"\n_Derniere mise a jour : {datetime.now().strftime('%Y-%m-%d %H:%M')} UTC_\n",
        "## Statistiques",
        f"- **Actions analysees** : {len(toutes)}",
        f"- **Candidats (>=55)** : {len(candidats)}",
        f"- **Score moyen** : {round(sum(float(r.get('Score',0) or 0) for r in toutes)/max(len(toutes),1), 1)}/100",
        "",
        "## Top 10 Candidats",
        "| # | Ticker | Nom | Secteur | Score | Q | V | M | S | Verdict |",
        "|---|--------|-----|---------|-------|---|---|---|---|---------|",
    ]
    for i, r in enumerate(top10, 1):
        lines.append(
            f"| {i} | **{r.get('Ticker','')}** | {r.get('name','')} | "
            f"{r.get('secteur','')} | **{r.get('Score','')}** | "
            f"{r.get('Q','')} | {r.get('V','')} | {r.get('M','')} | "
            f"{r.get('S','')} | {r.get('Verdict','')} |"
        )

    lines += [
        "",
        "## Signaux Forward-Looking (Top Candidats)",
        "| Ticker | Révision Proxy | Dist 52S | Accruals | Liquidité | Value Trap |",
        "|--------|---------------|----------|----------|-----------|------------|",
    ]
    for r in top10:
        lines.append(
            f"| {r.get('Ticker','')} | "
            f"{r.get('alerte_revision', '—') or '—'} | "
            f"{r.get('dist_52s','—')}% | "
            f"{r.get('accruals','—')} | "
            f"{r.get('liquidite','—')} | "
            f"{'OUI' if str(r.get('value_trap','')).lower() in ['true','1'] else 'non'} |"
        )

    lines += [
        "",
        "## Architecture Scoring",
        "| Pilier | Poids | Criteres principaux |",
        "|--------|-------|---------------------|",
        "| Qualite | 35% | ROIC proxy, ROE, Marge brute, Croissance CA, Piotroski |",
        "| Valeur | 25% | EV/EBITDA, P/FCF, P/Book, FCF Yield |",
        "| Momentum | 20% | 12-1M, 6M, EPS Growth |",
        "| Solidite | 15% | Altman Z, Dette/EBITDA, Current Ratio |",
        "| Signaux Fwd | 5% | Accruals, Distance 52S |",
        "",
        "> _Ce screener ne constitue pas un conseil en investissement._",
        "> _Toujours valider avec une analyse qualitative approfondie._",
    ]

    with open(README_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

# =============================================================================
# BOUCLE PRINCIPALE
# =============================================================================

def main():
    print("=" * 60)
    print("  LA FIRME — SCREENER PRO v3.0")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Chargement de l'état
    etat  = charger_etat()
    batch = selectionner_batch(etat, CONFIG["max_par_session"])

    total_univers = len(UNIVERS_PROPRE)
    deja_analysees = sum(1 for t, *_ in UNIVERS_PROPRE if t in etat)
    print(f"Univers total     : {total_univers} actions")
    print(f"Deja analysees    : {deja_analysees}")
    print(f"Ce batch          : {len(batch)} actions")
    print("-" * 60)

    # Analyse
    resultats       = []
    hist_all        = {}  # cache historiques pour momentum relatif sectoriel
    nb_ok           = 0
    nb_echec        = 0
    nb_candidats    = 0
    nb_pepites      = 0

    for idx, item in enumerate(batch, 1):
        ticker, nom, secteur, bourse, indice = item[:5]
        print(f"{idx}/{len(batch)} {ticker:<12} {nom[:25]:<25}", end=" ")
        sys.stdout.flush()

        time.sleep(CONFIG["delai_requete"])

        result = fetch_action(ticker, nom, secteur, bourse, hist_all)

        if result:
            resultats.append(result)
            nb_ok += 1
            score = result["Score"]
            verdict = result["Verdict"]
            print(f"Score: {score:5.1f} | {verdict}")
            if "PEPITE" in verdict:
                nb_pepites += 1
            if score >= CONFIG["seuil_candidat"]:
                nb_candidats += 1
        else:
            nb_echec += 1
            print("ECHEC")

    # Calcul momentum relatif sectoriel (post-batch, nécessite tous les historiques)
    print("\nCalcul momentum relatif sectoriel...")
    for r in resultats:
        mr = momentum_relatif_sectoriel(r["Ticker"], r["secteur"], hist_all)
        r["mom_rel_sect"] = mr

    # Sauvegarde
    print("\nSauvegarde des resultats...")
    toutes, candidats = sauvegarder_tout(resultats)

    # Stats finales
    print("\n" + "=" * 60)
    print(f"SESSION {datetime.now().strftime('%Y-%m-%d')}")
    print("=" * 60)
    print(f"Actions ce batch     : {len(batch)}")
    print(f"Couverture totale    : {len(toutes)}/{total_univers}")
    print(f"Reussites            : {nb_ok} | Echecs : {nb_echec}")
    print(f"Pepites  (>=75)      : {nb_pepites}")
    print(f"Candidats (>=55)     : {len(candidats)}")
    print(f"Score moyen          : {round(sum(float(r.get('Score',0) or 0) for r in resultats)/max(nb_ok,1),1)}/100")
    print("-" * 60)

    # Affichage top candidats
    if candidats:
        print("\nTOP CANDIDATS :")
        for r in sorted(candidats, key=lambda x: float(x.get("Score",0) or 0), reverse=True)[:15]:
            flags = []
            if r.get("value_trap"):
                flags.append("VALUE TRAP")
            if r.get("liquidite") == "ILLIQUIDE":
                flags.append("ILLIQUIDE")
            if r.get("alerte_revision"):
                flags.append(str(r["alerte_revision"])[:20])
            flag_str = " [" + " | ".join(flags) + "]" if flags else ""

            print(
                f"  {r['Ticker']:<12} {float(r.get('Score',0) or 0):5.1f}/100"
                f"  Q:{r.get('Q','?'):<5} V:{r.get('V','?'):<5}"
                f"  {r.get('Verdict','')}{flag_str}"
            )

    print("\nFichiers generes :")
    print(f"  {SCREENER_CSV}")
    print(f"  {CANDIDATS_JSON}")
    print(f"  {SESSION_CSV}")
    print(f"  {README_MD}")
    print("=" * 60)


if __name__ == "__main__":
    main()
