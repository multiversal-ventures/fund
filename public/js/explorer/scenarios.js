/**
 * Scoring scenarios and weight metadata — single source for Task 3+ (Task 4 adds Firestore save).
 */

export const SCENARIOS = {
  balanced: {
    m: { vacancy: 20, rentgrowth: 15, rent: 15, resilience: 20, hhi: 10, pop: 10, supply: 10 },
    d: { maturity: 40, units: 20, sec8: 20, vacancy: 20 },
    split: { market: 60, deal: 40 },
  },
  investor: {
    m: { vacancy: 15, rentgrowth: 10, rent: 10, resilience: 30, hhi: 15, pop: 10, supply: 10 },
    d: { maturity: 40, units: 20, sec8: 20, vacancy: 20 },
    split: { market: 80, deal: 20 },
  },
  ops: {
    m: { vacancy: 20, rentgrowth: 15, rent: 15, resilience: 20, hhi: 10, pop: 10, supply: 10 },
    d: { maturity: 50, units: 15, sec8: 25, vacancy: 10 },
    split: { market: 30, deal: 70 },
  },
  durable: {
    m: { vacancy: 15, rentgrowth: 25, rent: 10, resilience: 30, hhi: 10, pop: 10, supply: 0 },
    d: { maturity: 30, units: 20, sec8: 30, vacancy: 20 },
    split: { market: 70, deal: 30 },
  },
};

export const SCENARIO_LABELS = {
  balanced: 'Balanced',
  investor: 'Investor',
  ops: 'Ops',
  durable: 'Durable Tenants',
};

export const VIEW_QUERIES = {
  combined: 'SELECT * FROM properties ORDER BY total_score DESC LIMIT 50',
  investor:
    'SELECT fips, county, state, market_score, resilience_index, rent_growth, vacancy_trend, employment_concentration, supply_pressure FROM market_scores ORDER BY market_score DESC LIMIT 50',
  ops: 'SELECT fips, property_name, city, state, deal_score, maturity_years, units, section8, market_score FROM properties WHERE market_score >= 40 ORDER BY deal_score DESC LIMIT 50',
};

/** Market signal weights — order matches SCENARIOS.m keys */
export const MARKET_WEIGHT_META = [
  { domId: 'w-m-vacancy', mKey: 'vacancy', label: 'Vacancy Trend', explain: 'Falling county vacancy (tightening market) vs prior year — Census ACS.' },
  { domId: 'w-m-rentgrowth', mKey: 'rentgrowth', label: 'Rent Growth', explain: 'Median rent trajectory — demand signal.' },
  { domId: 'w-m-rent', mKey: 'rent', label: 'Rent/Cost Ratio', explain: 'Median rent vs owner cost — Day 1 cash-flow proxy.' },
  { domId: 'w-m-resilience', mKey: 'resilience', label: 'Workforce Resilience', explain: 'Share of jobs resistant to automation + AI disruption (Frey–Osborne + Eloundou).' },
  { domId: 'w-m-hhi', mKey: 'hhi', label: 'Employment HHI', explain: 'Industry concentration — lower = more diversified local economy.' },
  { domId: 'w-m-pop', mKey: 'pop', label: 'Pop Growth', explain: 'County population trend — renter demand.' },
  { domId: 'w-m-supply', mKey: 'supply', label: 'Supply Pressure', explain: 'New supply vs stock — permits pipeline.' },
];

export const DEAL_WEIGHT_META = [
  { domId: 'w-d-maturity', dKey: 'maturity', label: 'Mortgage Maturity', explain: 'FHA loan maturity window — motivated sellers when soon.' },
  { domId: 'w-d-units', dKey: 'units', label: 'Unit Count', explain: 'Scale per property — operating efficiency.' },
  { domId: 'w-d-sec8', dKey: 'sec8', label: 'Section 8', explain: 'Guaranteed rent / income stability.' },
  { domId: 'w-d-vacancy', dKey: 'vacancy', label: 'Area Vacancy', explain: 'Local rental vacancy — not too hot, not too cold.' },
];

/**
 * @param {keyof typeof SCENARIOS} key
 */
export function applyScenarioToDom(key) {
  const s = SCENARIOS[key];
  if (!s) return;
  document.getElementById('w-m-vacancy').value = s.m.vacancy;
  document.getElementById('w-m-rentgrowth').value = s.m.rentgrowth;
  document.getElementById('w-m-rent').value = s.m.rent;
  document.getElementById('w-m-resilience').value = s.m.resilience;
  document.getElementById('w-m-hhi').value = s.m.hhi;
  document.getElementById('w-m-pop').value = s.m.pop;
  document.getElementById('w-m-supply').value = s.m.supply;
  document.getElementById('w-d-maturity').value = s.d.maturity;
  document.getElementById('w-d-units').value = s.d.units;
  document.getElementById('w-d-sec8').value = s.d.sec8;
  document.getElementById('w-d-vacancy').value = s.d.vacancy;
  document.getElementById('w-split-market').value = s.split.market;
  document.getElementById('w-split-deal').value = s.split.deal;
}
