#!/usr/bin/env python3
# screener_auto.py - PEA Screener Automatique - GitHub Actions

import os, time, warnings
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

warnings.filterwarnings('ignore')

# -- Config --
ACTIONS_PAR_SESSION = 200
DELAI               = 0.7
SEUIL_CANDIDAT      = 55
RESULTATS_DIR       = Path("resultats")
RESULTATS_DIR.mkdir(exist_ok=True)

# -- Univers PEA --
UNIVERSE = {
    "CAC 40": [
        'AI.PA','AIR.PA','ALO.PA','ACA.PA','BNP.PA','EN.PA','CAP.PA',
        'CA.PA','BN.PA','DSY.PA','ENGI.PA','EL.PA','ERF.PA','RMS.PA',
        'KER.PA','LR.PA','OR.PA','MC.PA','ML.PA','ORA.PA','PUB.PA',
        'RI.PA','RNO.PA','SAF.PA','SGO.PA','SAN.PA','SU.PA','GLE.PA',
        'TTE.PA','VIE.PA','DG.PA','TEP.PA','HO.PA','WLN.PA','CS.PA',
    ],
    "SBF Mid": [
        'AC.PA','ADP.PA','AF.PA','BB.PA','FGR.PA','GTT.PA','MF.PA',
        'NK.PA','SOP.PA','TFI.PA','VK.PA','SPIE.PA','COFA.PA',
    ],
    "Europe": [
        'ASML.AS','PHIA.AS','HEIA.AS','AD.AS','INGA.AS',
        'SAP.DE','SIE.DE','ALV.DE','BMW.DE','BAYN.DE','MRK.DE',
        'NOVO-B.CO','DSV.CO',
        'ITX.MC','IBE.MC','BBVA.MC',
        'ENEL.MI','ISP.MI','RACE.MI',
        'ABI.BR','UCB.BR',
        'NOKIA.HE','NESTE.HE','EDP.LS',
    ],
}

TICKER_BOURSE = {t: b for b, tl in UNIVERSE.items() for t in tl}
ALL_TICKERS   = list({t: None for tl in UNIVERSE.values() for t in tl})

SECTOR_MAP = {
    'Technology': 'Tech', 'Information Technology': 'Tech', 'Semiconductors': 'Tech',
    'Health Care': 'Sante', 'Pharmaceuticals': 'Sante', 'Biotechnology': 'Sante',
    'Financials': 'Finance', 'Financial Services': 'Finance', 'Banking': 'Finance',
    'Consumer Discretionary': 'Conso', 'Consumer Staples': 'Conso',
    'Industrials': 'Industrie', 'Energy': 'Energie', 'Utilities': 'Utilities',
    'Materials': 'Materiaux', 'Communication Services': 'Telecom',
}

# -- Chargement etat existant --
STATE_FILE = RESULTATS_DIR / "screener_complet.csv"
if STATE_FILE.exists():
    df_existing = pd.read_csv(STATE_FILE)
    analyzed = {}
    for _, row in df_existing.iterrows():
        t = row.get('Ticker', '')
        d = str(row.get('Date_Analyse', ''))
        if t and d:
            try:
                analyzed[t] = datetime.strptime(d[:10], '%Y-%m-%d')
            except Exception:
                pass
else:
    df_existing = pd.DataFrame()
    analyzed    = {}

print("Etat : " + str(len(analyzed)) + "/" + str(len(ALL_TICKERS)) + " deja analysees")

# -- Selection batch du jour --
cutoff = datetime.now() - timedelta(days=30)
never  = [t for t in ALL_TICKERS if t not in analyzed]
old    = sorted(
    [t for t in ALL_TICKERS if t in analyzed and analyzed[t] < cutoff],
    key=lambda t: analyzed[t]
)
batch  = (never + old)[:ACTIONS_PAR_SESSION]
print("Batch du jour : " + str(len(batch)) + " actions (" + str(len(never)) + " nouvelles + " + str(len(old)) + " a renouveler)")

# -- Helpers --
def sf(v):
    try:
        f = float(v)
        return f if np.isfinite(f) else None
    except Exception:
        return None

def fetch(ticker):
    for _ in range(3):
        try:
            tk   = yf.Ticker(ticker)
            info = tk.info
            if not info or len(info) < 5:
                time.sleep(3)
                continue

            hist  = tk.history(period='2y')
            cap   = sf(info.get('marketCap'))
            ev    = sf(info.get('enterpriseValue'))
            ebitda= sf(info.get('ebitda'))
            rev   = sf(info.get('totalRevenue'))
            gross = sf(info.get('grossProfit'))
            ni    = sf(info.get('netIncome'))
            fcf   = sf(info.get('freeCashflow'))
            ta    = sf(info.get('totalAssets')) or 1
            debt  = sf(info.get('totalDebt')) or 0
            eq    = sf(info.get('bookValue'))
            sh    = sf(info.get('sharesOutstanding')) or 1
            tcl   = sf(info.get('totalCurrentLiabilities')) or 0
            tca   = sf(info.get('totalCurrentAssets')) or 0
            re    = sf(info.get('retainedEarnings')) or 0
            price = sf(info.get('currentPrice') or info.get('previousClose'))
            cr    = sf(info.get('currentRatio'))
            roe_v = sf(info.get('returnOnEquity'))
            gm_v  = sf(info.get('grossMargins'))
            rg_v  = sf(info.get('revenueGrowth'))
            eps   = sf(info.get('trailingEps'))
            epsf  = sf(info.get('forwardEps'))

            # Momentum 12-1M
            mom = None
            try:
                c = hist['Close'].dropna()
                if len(c) >= 200:
                    mom = round((float(c.iloc[-21]) - float(c.iloc[-252])) / float(c.iloc[-252]) * 100, 1)
            except Exception:
                pass

            # Piotroski
            pio = 0
            roa = ni / ta if ni else None
            if roa and roa > 0:                          pio += 1
            if fcf and fcf > 0:                          pio += 1
            if roe_v and roe_v > 0.05:                   pio += 1
            if fcf and ni and ni > 0 and fcf > ni * 0.7: pio += 1
            if debt / ta < 0.5:                          pio += 1
            if cr and cr >= 1.0:                         pio += 1
            if sh:                                       pio += 1
            if gm_v and gm_v > 0.15:                     pio += 1
            at = rev / ta if rev else None
            if at and at > 0.3:                          pio += 1

            # Altman Z
            wc  = tca - tcl
            eb  = sf(info.get('ebitda')) or 0
            alt = round(
                1.2*(wc/ta) + 1.4*(re/ta) + 3.3*(eb/ta) +
                0.6*((cap or 0)/(debt or 1)) + 1.0*((rev or 0)/ta), 2
            )

            secteur = SECTOR_MAP.get(info.get('sector', ''), 'Autre')
            if cap and cap >= 10e9:   cap_cat = 'Large'
            elif cap and cap >= 2e9:  cap_cat = 'Mid'
            else:                      cap_cat = 'Small'

            return {
                'name':      info.get('longName') or ticker,
                'secteur':   secteur,
                'cap':       cap_cat,
                'prix':      price,
                'devise':    info.get('currency', 'EUR'),
                'roic':      round((ni / max(ta - tcl, 1)) * 100, 1) if ni else None,
                'roe':       round(roe_v * 100, 1)                   if roe_v else None,
                'gm':        round(gm_v * 100, 1)                    if gm_v else None,
                'rg':        round(rg_v * 100, 1)                    if rg_v else None,
                'ev_ebitda': round(ev / ebitda, 1)                   if ev and ebitda and ebitda > 0 else None,
                'pfcf':      round(price / (fcf / sh), 1)            if price and fcf and fcf > 0 else None,
                'pb':        round(price / eq, 2)                    if price and eq and eq > 0 else None,
                'fcy':       round((fcf / cap) * 100, 1)             if fcf and cap else None,
                'mom':       mom,
                'epsg':      round(((epsf - eps) / abs(eps)) * 100, 1) if eps and epsf and eps != 0 else None,
                'altman':    alt,
                'debt_eb':   round(debt / ebitda, 2)                 if ebitda and ebitda > 0 else None,
                'cur_r':     cr,
                'pio':       pio,
            }
        except Exception:
            time.sleep(3)
    return None

# -- Fetch --
start  = datetime.now()
raws   = []
errors = []

for i, ticker in enumerate(batch):
    print(str(i + 1) + "/" + str(len(batch)) + " " + ticker, end='\r')
    d = fetch(ticker)
    if d:
        raws.append({'Ticker': ticker, 'Bourse': TICKER_BOURSE.get(ticker, '?'), **d})
    else:
        errors.append(ticker)
    time.sleep(DELAI)

elapsed = round((datetime.now() - start).seconds / 60, 1)
print("\nFetch termine en " + str(elapsed) + "min | OK=" + str(len(raws)) + " | Echecs=" + str(len(errors)))

# -- Scoring z-scores --
def z100(v, s, hib=True):
    try:
        c = pd.to_numeric(s, errors='coerce').dropna()
        if len(c) < 3:
            return 50.0
        mu, sig = c.mean(), c.std()
        if sig == 0:
            return 50.0
        z = max(-3.0, min(3.0, (v - mu) / sig))
        if not hib:
            z = -z
        return round((z + 3) / 6 * 100, 1)
    except Exception:
        return 50.0

def score_action(row, df_sec):
    def z(col, hib=True):
        v = row.get(col)
        if v is None:
            return 45.0
        s = df_sec[col] if col in df_sec.columns else pd.Series([v])
        return z100(v, s, hib)

    q = np.mean([
        z('roic') if row.get('roic') else z('roe'),
        z('gm'), z('rg'),
        (row.get('pio') or 0) / 9 * 100
    ])
    v = np.mean([z('ev_ebitda', False), z('pfcf', False), z('pb', False), z('fcy')])
    m = np.mean([z('mom'), z('epsg')])

    alt = row.get('altman')
    deb = row.get('debt_eb')
    cr  = row.get('cur_r')

    s_alt = 85 if (alt and alt > 2.99) else 50 if (alt and alt > 1.81) else 20 if alt else 45
    s_deb = 80 if (deb and deb < 1) else 65 if (deb and deb < 2) else 40 if (deb and deb < 3) else 20 if deb else 45
    s_cr  = 80 if (cr and cr > 2)   else 65 if (cr and cr >= 1)  else 25 if cr else 45
    s_fin = np.mean([s_alt, s_deb, s_cr])

    tot  = round(q * 0.35 + v * 0.30 + m * 0.20 + s_fin * 0.15, 1)

    if   tot >= 75: verd = 'PEPITE (>=75)'
    elif tot >= 55: verd = 'CANDIDAT (>=55)'
    elif tot >= 40: verd = 'SURVEILLER (>=40)'
    else:           verd = 'EVITER (<40)'

    return round(q, 1), round(v, 1), round(m, 1), round(s_fin, 1), tot, verd

# -- Calcul scores --
if raws:
    df = pd.DataFrame(raws)
    NUM = ['roic','roe','gm','rg','ev_ebitda','pfcf','pb','fcy','mom','epsg']
    for c in NUM:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')

    scored = []
    for _, row in df.iterrows():
        df_sec = df[df['secteur'] == row['secteur']]
        if len(df_sec.dropna(how='all')) < 4:
            df_sec = df
        q, v, m, s, tot, verd = score_action(row.to_dict(), df_sec)
        scored.append({
            **row.to_dict(),
            'Q': q, 'V': v, 'M': m, 'S': s,
            'Score': tot, 'Verdict': verd,
            'Date_Analyse': datetime.now().strftime('%Y-%m-%d')
        })

    df_session = pd.DataFrame(scored).sort_values('Score', ascending=False)

    # Fusion avec donnees existantes
    if not df_existing.empty:
        df_merged = pd.concat([df_existing, df_session])
        df_merged = df_merged.drop_duplicates(subset=['Ticker'], keep='last')
    else:
        df_merged = df_session

    df_merged = df_merged.sort_values('Score', ascending=False)

    today = datetime.now().strftime('%Y-%m-%d')

    # Sauvegarde CSV complet
    df_merged.to_csv(RESULTATS_DIR / "screener_complet.csv", index=False, encoding='utf-8-sig')

    # CSV session du jour
    df_session.to_csv(RESULTATS_DIR / ("session_" + today + ".csv"), index=False, encoding='utf-8-sig')

    # JSON candidats
    candidats = df_merged[df_merged['Score'] >= SEUIL_CANDIDAT]
    candidats.to_json(RESULTATS_DIR / "candidats.json", orient='records', force_ascii=False)

    # README
    pepites = df_merged[df_merged['Score'] >= 75]
    cands   = df_merged[(df_merged['Score'] >= 55) & (df_merged['Score'] < 75)]

    lines = []
    lines.append("# PEA Screener - Resultats")
    lines.append("")
    lines.append("Derniere mise a jour : " + today)
    lines.append("Actions analysees : " + str(len(df_merged)) + " / " + str(len(ALL_TICKERS)))
    lines.append("")
    lines.append("## Top 10")
    lines.append("")
    lines.append("| Ticker | Nom | Score | Verdict |")
    lines.append("|--------|-----|-------|---------|")
    for _, r in df_merged.head(10).iterrows():
        nom = str(r.get('name', r['Ticker']))[:30]
        lines.append("| " + r['Ticker'] + " | " + nom + " | " + str(r['Score']) + "/100 | " + str(r['Verdict']) + " |")
    lines.append("")
    lines.append("## Resume")
    lines.append("")
    lines.append("- Pepites (>=75) : " + str(len(pepites)))
    lines.append("- Candidats (>=55) : " + str(len(cands)))
    lines.append("- Score moyen : " + str(round(df_merged['Score'].mean(), 1)) + "/100")
    lines.append("")
    lines.append("## Fichiers")
    lines.append("")
    lines.append("- `resultats/screener_complet.csv` - Toutes les actions")
    lines.append("- `resultats/candidats.json` - Actions >= " + str(SEUIL_CANDIDAT) + " pts")
    lines.append("- `resultats/session_" + today + ".csv` - Session du jour")

    (RESULTATS_DIR / "README.md").write_text('\n'.join(lines), encoding='utf-8')

    # Resume console
    print("")
    print("=" * 50)
    print("SESSION " + today)
    print("=" * 50)
    print("Actions cette session : " + str(len(df_session)))
    print("Couverture totale     : " + str(len(df_merged)) + "/" + str(len(ALL_TICKERS)))
    print("Pepites  (>=75)       : " + str(len(pepites)))
    print("Candidats (>=55)      : " + str(len(cands)))
    print("Score moyen           : " + str(round(df_merged['Score'].mean(), 1)) + "/100")
    print("Duree                 : " + str(elapsed) + " min")
    print("=" * 50)

    if not candidats.empty:
        print("")
        print("CANDIDATS >= " + str(SEUIL_CANDIDAT) + "pts :")
        for _, r in candidats.iterrows():
            print("  " + str(r['Ticker']).ljust(14) + str(r['Score']).rjust(5) + "/100  " + str(r['Verdict']))

else:
    print("Aucune donnee recue - verifier la connexion Yahoo Finance")
