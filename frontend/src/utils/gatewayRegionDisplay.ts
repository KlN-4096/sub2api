// Display-only buckets. They never merge per-gateway cooldown or contact state.
export const GATEWAY_REGION_KEYS = [
  'north-america', 'south-america', 'europe',
  'east-asia', 'southeast-asia', 'south-asia', 'middle-east',
  'africa', 'oceania', ''
] as const

const countryGroups = new Map<string, string>()
for (const [key, countries] of [
  ['south-america', 'AR BO BR CL CO EC FK GF GY PE PY SR UY VE'],
  ['europe', 'AD AL AM AT AX AZ BA BE BG BY CH CY CZ DE DK EE ES FI FO FR GB GE GG GI GR HR HU IE IM IS IT JE LI LT LU LV MC MD ME MK MT NL NO PL PT RO RS RU SE SI SJ SK SM UA VA'],
  ['east-asia', 'CN HK JP KP KR MN MO TW'],
  ['south-asia', 'AF BD BT IN LK MV NP PK'],
  ['southeast-asia', 'BN ID KH LA MM MY PH SG TH TL VN'],
  ['middle-east', 'AE BH IL IQ IR JO KG KW KZ LB OM PS QA SA SY TJ TM TR UZ YE'],
  ['africa', 'AO BF BI BJ BW CD CF CG CI CM CV DJ DZ EG EH ER ET GA GH GM GN GQ GW KE KM LR LS LY MA MG ML MR MU MW MZ NA NE NG RE RW SC SD SH SL SN SO SS ST SZ TD TF TG TN TZ UG YT ZA ZM ZW'],
  ['north-america', 'AG AI AW BB BL BM BQ BS BZ CA CR CU CW DM DO GD GL GP GT HN HT JM KN KY LC MF MQ MS MX NI PA PM PR SV SX TC TT US VC VG VI'],
  ['oceania', 'AS AU CC CK CX FJ FM GU KI MH MP NC NF NR NU NZ PF PG PN PW SB TK TO TV UM VU WF WS']
]) {
  for (const country of countries.split(' ')) countryGroups.set(country, key)
}

export function gatewayRegionDisplayKey(exitKey: string): string {
  if (exitKey === 'us-east' || exitKey === 'us-west') return 'north-america'
  if (exitKey === 'west-europe') return 'europe'
  if ((GATEWAY_REGION_KEYS as readonly string[]).includes(exitKey)) return exitKey
  if (exitKey === 'de-central') return 'europe'
  if (exitKey === 'southeast-asia-sg') return 'southeast-asia'
  if (exitKey === 'africa-south') return 'africa'
  if (/^country-[a-z]{2}$/.test(exitKey)) {
    return countryGroups.get(exitKey.slice('country-'.length).toUpperCase()) ?? ''
  }
  // Do not guess the geography of arbitrary user-defined keys.
  return ''
}
