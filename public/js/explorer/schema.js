/**
 * Explorer table/column metadata for SQL Studio sidebar + CodeMirror completion.
 * Aligns with Parquet-backed tables in `duckdb.js`.
 */

/** @typedef {{ name: string, type: string, description: string, tooltip: string }} SchemaColumn */
/** @typedef {{ name: string, description: string, approxRows?: number, columns: SchemaColumn[] }} SchemaTable */

/** @type {SchemaTable[]} */
export const SCHEMA_TABLES = [
  {
    name: 'properties',
    description: 'Scored acquisition targets (HUD FHA subset with model scores).',
    approxRows: 434,
    columns: [
      { name: 'fips', type: 'VARCHAR', description: '5-digit county FIPS', tooltip: 'Join key to census and market_scores.' },
      { name: 'property_name', type: 'VARCHAR', description: 'Property label', tooltip: '' },
      { name: 'address', type: 'VARCHAR', description: 'Street address', tooltip: '' },
      { name: 'city', type: 'VARCHAR', description: 'City', tooltip: '' },
      { name: 'state', type: 'VARCHAR', description: 'State abbreviation', tooltip: '' },
      { name: 'zip', type: 'VARCHAR', description: 'ZIP', tooltip: '' },
      { name: 'units', type: 'INTEGER', description: 'Unit count', tooltip: '' },
      { name: 'maturity_date', type: 'DATE', description: 'Loan maturity', tooltip: '' },
      { name: 'maturity_years', type: 'DOUBLE', description: 'Years to maturity', tooltip: '' },
      { name: 'section8', type: 'BOOLEAN', description: 'Section 8', tooltip: '' },
      { name: 'lat', type: 'DOUBLE', description: 'Latitude', tooltip: '' },
      { name: 'lng', type: 'DOUBLE', description: 'Longitude', tooltip: '' },
      { name: 'deal_score', type: 'DOUBLE', description: 'Deal layer score', tooltip: '' },
      { name: 'market_score', type: 'DOUBLE', description: 'County market score', tooltip: '' },
      { name: 'total_score', type: 'DOUBLE', description: 'Weighted combined score', tooltip: '' },
      { name: 'signal_rank', type: 'INTEGER', description: 'Signal rank', tooltip: '' },
      { name: 'stability_score', type: 'DOUBLE', description: 'Stability', tooltip: '' },
      { name: 'rank_std', type: 'DOUBLE', description: 'Monte Carlo std', tooltip: '' },
      { name: 'rank_min', type: 'DOUBLE', description: 'Monte Carlo min', tooltip: '' },
      { name: 'rank_max', type: 'DOUBLE', description: 'Monte Carlo max', tooltip: '' },
      { name: 'zillow_url', type: 'VARCHAR', description: 'Zillow link if present', tooltip: '' },
    ],
  },
  {
    name: 'market_scores',
    description: 'County-level market scores and signal components.',
    approxRows: 1044,
    columns: [
      { name: 'fips', type: 'VARCHAR', description: 'County FIPS', tooltip: '' },
      { name: 'county', type: 'VARCHAR', description: 'County name', tooltip: '' },
      { name: 'state', type: 'VARCHAR', description: 'State', tooltip: '' },
      { name: 'market_score', type: 'DOUBLE', description: 'Composite market score', tooltip: '' },
      { name: 'vacancy_trend', type: 'DOUBLE', description: 'Vacancy trend signal', tooltip: 'Falling vacancy vs prior year — Census ACS.' },
      { name: 'rent_growth', type: 'DOUBLE', description: 'Rent growth', tooltip: '' },
      { name: 'rent_cost_ratio', type: 'DOUBLE', description: 'Rent vs owner cost', tooltip: '' },
      { name: 'resilience_index', type: 'DOUBLE', description: 'AI / automation resilience', tooltip: 'Frey–Osborne + Eloundou blend.' },
      { name: 'employment_concentration', type: 'DOUBLE', description: 'Employment HHI', tooltip: 'Industry concentration; lower = more diversified.' },
      { name: 'pop_growth', type: 'DOUBLE', description: 'Population growth', tooltip: '' },
      { name: 'supply_pressure', type: 'DOUBLE', description: 'Supply pipeline pressure', tooltip: '' },
    ],
  },
  {
    name: 'census_2023',
    description: 'ACS housing & population by county (2023).',
    approxRows: 3222,
    columns: [
      { name: 'fips', type: 'VARCHAR', description: 'County FIPS', tooltip: '' },
      { name: 'county', type: 'VARCHAR', description: 'County', tooltip: '' },
      { name: 'state', type: 'VARCHAR', description: 'State', tooltip: '' },
      { name: 'total_units', type: 'BIGINT', description: 'Housing units', tooltip: '' },
      { name: 'occupied', type: 'BIGINT', description: 'Occupied units', tooltip: '' },
      { name: 'vacant', type: 'BIGINT', description: 'Vacant units', tooltip: '' },
      { name: 'owner_occupied', type: 'BIGINT', description: 'Owner-occupied', tooltip: '' },
      { name: 'renter_occupied', type: 'BIGINT', description: 'Renter-occupied', tooltip: '' },
      { name: 'median_rent', type: 'DOUBLE', description: 'Median gross rent', tooltip: '' },
      { name: 'median_home_value', type: 'DOUBLE', description: 'Median home value', tooltip: '' },
      { name: 'median_owner_cost', type: 'DOUBLE', description: 'Median owner cost', tooltip: '' },
      { name: 'mf_units', type: 'BIGINT', description: 'Multifamily units', tooltip: '' },
      { name: 'mf_pct', type: 'DOUBLE', description: 'Multifamily share', tooltip: '' },
      { name: 'pop', type: 'BIGINT', description: 'Population', tooltip: '' },
      { name: 'vacancy_rate', type: 'DOUBLE', description: 'Vacancy rate', tooltip: '' },
      { name: 'rental_vac_rate', type: 'DOUBLE', description: 'Rental vacancy rate', tooltip: '' },
      { name: 'rent_to_cost_ratio', type: 'DOUBLE', description: 'Rent to cost ratio', tooltip: '' },
      { name: 'year', type: 'INTEGER', description: 'Survey year', tooltip: '' },
    ],
  },
  {
    name: 'census_2022',
    description: 'ACS 2022 — same schema as census_2023.',
    approxRows: 3222,
    columns: [], // use census_2023 columns
  },
  {
    name: 'census_2021',
    description: 'ACS 2021 — same schema as census_2023.',
    approxRows: 3222,
    columns: [],
  },
  {
    name: 'occupations_2023',
    description: 'County workforce / AI-resilience index.',
    approxRows: 3222,
    columns: [
      { name: 'fips', type: 'VARCHAR', description: 'County FIPS', tooltip: '' },
      { name: 'county', type: 'VARCHAR', description: 'County', tooltip: '' },
      { name: 'state', type: 'VARCHAR', description: 'State', tooltip: '' },
      { name: 'year', type: 'INTEGER', description: 'Year', tooltip: '' },
      { name: 'total_employed', type: 'BIGINT', description: 'Employed count', tooltip: '' },
      { name: 'resilience_index', type: 'DOUBLE', description: 'Workforce resilience', tooltip: '' },
      { name: 'blue_collar_safe_pct', type: 'DOUBLE', description: 'Blue-collar safe share', tooltip: '' },
      { name: 'white_collar_risk_pct', type: 'DOUBLE', description: 'White-collar AI risk share', tooltip: '' },
      { name: 'healthcare_pct', type: 'DOUBLE', description: 'Healthcare employment share', tooltip: '' },
    ],
  },
  {
    name: 'occupations_2022',
    description: 'Same schema as occupations_2023 (2022).',
    approxRows: 3222,
    columns: [],
  },
  {
    name: 'occupations_2021',
    description: 'Same schema as occupations_2023 (2021).',
    approxRows: 3222,
    columns: [],
  },
  {
    name: 'cbp_2023',
    description: 'County Business Patterns — employment diversification (HHI).',
    approxRows: 3237,
    columns: [
      { name: 'fips', type: 'VARCHAR', description: 'County FIPS', tooltip: 'Join to census_2023 for county name.' },
      { name: 'state', type: 'VARCHAR', description: 'State FIPS', tooltip: '' },
      { name: 'year', type: 'INTEGER', description: 'Year', tooltip: '' },
      { name: 'total_employment', type: 'BIGINT', description: 'Total employment', tooltip: '' },
      { name: 'hhi', type: 'DOUBLE', description: 'Herfindahl index', tooltip: 'Lower = more diversified.' },
      { name: 'top_sector_name', type: 'VARCHAR', description: 'Largest sector', tooltip: '' },
      { name: 'top_sector_share', type: 'DOUBLE', description: 'Top sector share', tooltip: '' },
      { name: 'top3_share', type: 'DOUBLE', description: 'Top 3 sectors share', tooltip: '' },
      { name: 'num_sectors', type: 'INTEGER', description: 'Sector count', tooltip: '' },
    ],
  },
  {
    name: 'permits_2023',
    description: 'Building permits / supply pipeline.',
    approxRows: 3029,
    columns: [
      { name: 'fips', type: 'VARCHAR', description: 'County FIPS', tooltip: '' },
      { name: 'state', type: 'VARCHAR', description: 'State', tooltip: '' },
      { name: 'county', type: 'VARCHAR', description: 'County name', tooltip: '' },
      { name: 'year', type: 'INTEGER', description: 'Year', tooltip: '' },
      { name: 'total_permits', type: 'BIGINT', description: 'Total permits', tooltip: '' },
      { name: 'total_units_permitted', type: 'BIGINT', description: 'Units permitted', tooltip: '' },
      { name: 'sf_permits', type: 'BIGINT', description: 'Single-family permits', tooltip: '' },
      { name: 'mf_permits', type: 'BIGINT', description: 'Multifamily permits', tooltip: '' },
      { name: 'mf_units_permitted', type: 'BIGINT', description: 'MF units', tooltip: '' },
      { name: 'mf_pct', type: 'DOUBLE', description: 'MF share of units', tooltip: '' },
    ],
  },
  {
    name: 'hud_fha',
    description: 'HUD FHA insured multifamily — raw inventory.',
    approxRows: 17484,
    columns: [
      { name: 'fips', type: 'VARCHAR', description: 'County FIPS', tooltip: '' },
      { name: 'property_name', type: 'VARCHAR', description: 'Name', tooltip: '' },
      { name: 'address', type: 'VARCHAR', description: 'Address', tooltip: '' },
      { name: 'city', type: 'VARCHAR', description: 'City', tooltip: '' },
      { name: 'state', type: 'VARCHAR', description: 'State', tooltip: '' },
      { name: 'zip', type: 'VARCHAR', description: 'ZIP', tooltip: '' },
      { name: 'units', type: 'INTEGER', description: 'Units', tooltip: '' },
      { name: 'maturity_date', type: 'DATE', description: 'Maturity', tooltip: '' },
      { name: 'maturity_years', type: 'DOUBLE', description: 'Years to maturity', tooltip: '' },
      { name: 'section8', type: 'BOOLEAN', description: 'Section 8', tooltip: '' },
      { name: 'lat', type: 'DOUBLE', description: 'Latitude', tooltip: '' },
      { name: 'lng', type: 'DOUBLE', description: 'Longitude', tooltip: '' },
      { name: 'loan_id', type: 'VARCHAR', description: 'Loan id', tooltip: '' },
    ],
  },
  {
    name: 'usps_vacancy',
    description: 'USPS quarterly residential vacancy.',
    approxRows: 0,
    columns: [
      { name: 'fips', type: 'VARCHAR', description: 'County FIPS', tooltip: '' },
      { name: 'vacancy_rate', type: 'DOUBLE', description: 'Vacancy rate', tooltip: '' },
      { name: 'year', type: 'INTEGER', description: 'Year', tooltip: '' },
    ],
  },
  {
    name: 'bls_2023',
    description: 'BLS OEWS — employment & wages by metro × occupation.',
    approxRows: 48559,
    columns: [
      { name: 'metro_code', type: 'VARCHAR', description: 'Metro code', tooltip: '' },
      { name: 'metro_name', type: 'VARCHAR', description: 'Metro name', tooltip: '' },
      { name: 'occ_code', type: 'VARCHAR', description: 'SOC code', tooltip: '' },
      { name: 'occ_title', type: 'VARCHAR', description: 'Occupation title', tooltip: '' },
      { name: 'total_employment', type: 'BIGINT', description: 'Employment', tooltip: '' },
      { name: 'hourly_median', type: 'DOUBLE', description: 'Median hourly wage', tooltip: '' },
      { name: 'annual_median', type: 'DOUBLE', description: 'Median annual wage', tooltip: '' },
      { name: 'location_quotient', type: 'DOUBLE', description: 'Location quotient', tooltip: '' },
      { name: 'year', type: 'INTEGER', description: 'Year', tooltip: '' },
    ],
  },
];

const CENSUS23 = SCHEMA_TABLES.find((t) => t.name === 'census_2023');
const OCC23 = SCHEMA_TABLES.find((t) => t.name === 'occupations_2023');

for (const t of SCHEMA_TABLES) {
  if (t.columns.length === 0) {
    if (t.name.startsWith('census_') && CENSUS23) t.columns = CENSUS23.columns;
    else if (t.name.startsWith('occupations_') && OCC23) t.columns = OCC23.columns;
  }
}

/**
 * CodeMirror @codemirror/lang-sql `schema` object: table name → { columns }.
 */
export function getCodemirrorSqlSchema() {
  /** @type {Record<string, { columns: { name: string, type?: string }[] }>} */
  const out = {};
  for (const t of SCHEMA_TABLES) {
    if (!t.columns.length) continue;
    out[t.name] = {
      columns: t.columns.map((c) => ({ name: c.name, type: c.type })),
    };
  }
  return out;
}

function esc(s) {
  if (s == null) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/**
 * @param {string} q search string (lowercase)
 * @param {SchemaTable} table
 */
function tableMatchesQuery(q, table) {
  if (!q) return true;
  if (table.name.toLowerCase().includes(q)) return true;
  if (table.description.toLowerCase().includes(q)) return true;
  return table.columns.some(
    (c) =>
      c.name.toLowerCase().includes(q) ||
      c.description.toLowerCase().includes(q) ||
      (c.tooltip && c.tooltip.toLowerCase().includes(q)),
  );
}

/**
 * @param {string} searchRaw
 * @returns {string} HTML
 */
export function renderSchemaSidebarHtml(searchRaw = '') {
  const q = searchRaw.trim().toLowerCase();
  return SCHEMA_TABLES.filter((t) => tableMatchesQuery(q, t))
    .map((table) => {
      const cols = !q
        ? table.columns
        : table.columns.filter(
            (c) =>
              c.name.toLowerCase().includes(q) ||
              c.description.toLowerCase().includes(q) ||
              (c.tooltip && c.tooltip.toLowerCase().includes(q)) ||
              table.name.toLowerCase().includes(q),
          );
      const rowHint =
        table.approxRows != null ? `<span class="schema-table-count">~${table.approxRows.toLocaleString()} rows</span>` : '';
      return `<section class="schema-table-block" data-table="${esc(table.name)}">
  <button type="button" class="schema-table-header" aria-expanded="true">
    <span class="schema-table-icon">▾</span>
    <span class="schema-table-name">${esc(table.name)}</span>
    ${rowHint}
  </button>
  <p class="schema-table-desc">${esc(table.description)}</p>
  <ul class="schema-col-list">
    ${cols
      .map(
        (c) => `<li class="schema-col-row">
      <span class="schema-col-name">${esc(c.name)}</span>
      <span class="schema-col-type">${esc(c.type)}</span>
      <span class="schema-col-info" title="${esc(c.tooltip || c.description)}">ⓘ</span>
    </li>`,
      )
      .join('')}
  </ul>
</section>`;
    })
    .join('');
}
