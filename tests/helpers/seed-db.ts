import { createDatabase, type Database } from '../../src/db.js';

export function createSeededDatabase(dbPath: string): Database {
  const db = createDatabase(dbPath);

  // --- Rotation guidance ---
  const rotations = [
    ['winter wheat', 'oilseed rape', 'good', 'Break crop benefit, different disease spectrum', null, 'medium', 7.5, 'AHDB rotation guidance'],
    ['oilseed rape', 'winter wheat', 'good', 'OSR is a good entry for first wheat, typical UK rotation', null, 'medium', 0, 'AHDB rotation guidance'],
    ['winter wheat', 'winter wheat', 'acceptable', 'Second wheat yield penalty, take-all risk increases', null, 'high', -12.5, 'AHDB rotation guidance'],
    ['winter wheat', 'winter wheat continuous', 'poor', 'Continuous wheat: take-all builds, blackgrass risk very high', null, 'very high', -20, 'AHDB rotation guidance'],
    ['winter wheat', 'beans', 'good', 'Beans fix nitrogen, provide disease break from cereals', null, 'low', 5, 'AHDB rotation guidance'],
    ['beans', 'winter wheat', 'good', 'N fixation benefit reduces fertiliser need for following wheat', null, 'low', 7, 'AHDB rotation guidance'],
    ['winter barley', 'winter wheat', 'good', 'Early harvest gives good entry, different disease break', null, 'medium', 0, 'AHDB rotation guidance'],
    ['spring barley', 'winter wheat', 'good', 'Spring cropping breaks blackgrass cycle effectively', null, 'low', 0, 'AHDB rotation guidance'],
    ['oilseed rape', 'oilseed rape', 'avoid', 'Minimum 4-year break between OSR crops required for clubroot and sclerotinia', 4, 'medium', -30, 'AHDB rotation guidance'],
    ['sugar beet', 'winter wheat', 'good', 'Deep rooting crop, different herbicide group, good break', null, 'low', 0, 'AHDB rotation guidance'],
    ['potatoes', 'winter wheat', 'good', 'Good break but requires 5-year rotation for PCN and blight control', 5, 'low', 0, 'AHDB rotation guidance'],
    ['linseed', 'winter wheat', 'good', 'Good break crop, low input, suits lighter soils', null, 'low', 0, 'AHDB rotation guidance'],
    ['maize', 'winter wheat', 'good', 'No disease interaction with cereals, good for livestock farms', null, 'low', 0, 'AHDB rotation guidance'],
  ];

  for (const [crop, following, suit, reason, diseaseBreak, bgRisk, yieldImpact, source] of rotations) {
    db.run(
      `INSERT INTO rotation_guidance (crop, following_crop, suitability, reason, disease_break_years, blackgrass_risk, yield_impact_pct, source, jurisdiction)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'GB')`,
      [crop, following, suit, reason, diseaseBreak, bgRisk, yieldImpact, source]
    );
  }

  // --- Gross margins (2024/25 estimates, AHDB Farmbench style) ---
  const margins = [
    ['Winter wheat', '2024/25', 1530, 650, 880, 'per_ha', 1100, 600, 'AHDB Farmbench 2024/25'],
    ['Winter barley', '2024/25', 1250, 580, 670, 'per_ha', 850, 450, 'AHDB Farmbench 2024/25'],
    ['Spring barley', '2024/25', 1100, 500, 600, 'per_ha', 800, 380, 'AHDB Farmbench 2024/25'],
    ['Oilseed rape', '2024/25', 1200, 550, 650, 'per_ha', 900, 350, 'AHDB Farmbench 2024/25'],
    ['Winter beans', '2024/25', 900, 350, 550, 'per_ha', 700, 350, 'AHDB Farmbench 2024/25'],
    ['Sugar beet', '2024/25', 2200, 900, 1300, 'per_ha', 1600, 900, 'AHDB Farmbench 2024/25'],
    ['Potatoes (maincrop)', '2024/25', 5500, 3200, 2300, 'per_ha', 3500, 800, 'AHDB Farmbench 2024/25'],
    ['Dairy cow', '2024/25', 2800, 1900, 900, 'per_head', 1300, 400, 'AHDB Pocketbook 2024/25'],
    ['Beef finisher', '2024/25', 1600, 1200, 400, 'per_head', 600, 100, 'AHDB Pocketbook 2024/25'],
    ['Lowland ewe', '2024/25', 145, 65, 80, 'per_head', 120, 30, 'AHDB Pocketbook 2024/25'],
    ['Upland ewe', '2024/25', 110, 50, 60, 'per_head', 90, 20, 'AHDB Pocketbook 2024/25'],
    ['Broilers (per 1000)', '2024/25', 9500, 8200, 1300, 'per_1000_birds', 1800, 600, 'AHDB Pocketbook 2024/25'],
    ['Free-range eggs (per bird)', '2024/25', 24, 18, 6, 'per_bird', 9, 2, 'AHDB Pocketbook 2024/25'],
  ];

  for (const [ent, year, output, vc, gm, unit, top, bottom, source] of margins) {
    db.run(
      `INSERT INTO gross_margins (enterprise, year, output_per_unit, variable_costs_per_unit, gross_margin_per_unit, unit, top_quartile, bottom_quartile, source, jurisdiction)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'GB')`,
      [ent, year, output, vc, gm, unit, top, bottom, source]
    );
  }

  // --- Tax rules ---
  const taxRules = [
    ['Making Tax Digital (Income Tax)', 'MTD for Income Tax Self Assessment mandatory from April 2026 for sole traders and landlords with income over £50,000. Quarterly updates via compatible software. Threshold drops to £30,000 from April 2027.', 'Gross income from self-employment or property exceeds threshold', 'Quarterly updates due by 5th of month following quarter end; final declaration by 31 January', 'Points-based penalty system: 1 point per late submission, £200 penalty at threshold (varies by submission frequency)', 'HMRC MTD-ITSA guidance'],
    ['Making Tax Digital (VAT)', 'MTD for VAT already mandatory for all VAT-registered businesses. Must keep digital records and submit returns via compatible software.', 'All VAT-registered businesses', 'Quarterly VAT returns', 'Default surcharge for late returns; interest on late payments', 'VAT Notice 700/22'],
    ['Farmers averaging', 'Farmers can average profits over 2 years (standard) or 5 years (from 2016/17 onwards). Claim must be made within first anniversary of the normal self-assessment filing date for the later tax year. Averaging smooths volatile farming income for income tax purposes.', 'Farming profits only (not mixed income). Must have at least 2 or 5 consecutive years of farming.', 'Claim within 12 months of 31 January filing deadline for later year', 'No specific penalty for not claiming — it is optional. Incorrect claims may incur interest.', 'ITTOIA 2005 ss 221-225 / HMRC BIM84000'],
    ['Capital allowances', 'Annual Investment Allowance (AIA) of £1,000,000 for qualifying plant and machinery. 100% first-year allowance on qualifying assets. Full expensing at 100% for main rate assets from April 2023 (companies only).', 'Must be used in the trade. Buildings do not qualify (use SBA instead). Cars have separate rules.', 'Claim in the tax year the expenditure is incurred', 'N/A', 'CAA 2001 / HMRC CA20000'],
    ['Structures and Buildings Allowance', 'SBA provides 3% per year straight-line relief over 33.33 years for qualifying new commercial structures and buildings, including farm buildings. Available from October 2018.', 'New construction or renovation of commercial buildings. Must have a valid allowance statement.', 'Relief claimed annually over 33.33 years from the date the building comes into use', 'N/A', 'HMRC CA90000'],
    ['Flat rate VAT scheme', 'Agricultural flat rate scheme: farmers can use a 4% flat rate addition on sales instead of standard VAT accounting. Alternatively, the general flat rate scheme rate for agriculture is 6.5%. Beneficial when input VAT is relatively low.', 'Must apply to HMRC. Cannot reclaim input VAT under flat rate.', 'Quarterly or annual returns as agreed with HMRC', 'Standard VAT penalties apply', 'VAT Notice 733 / VAT Notice 700/46'],
    ['Partnership tax', 'Each partner in a farming partnership files their own self-assessment return. Profit allocation follows the partnership agreement. Partnership itself files a partnership return (SA800).', 'Must have a valid partnership agreement. All partners must register for self-assessment.', 'Partnership return due 31 January following the end of the tax year', 'Late filing penalty £100 per partner; £10/day after 3 months; further penalties at 6 and 12 months', 'HMRC Partnership Manual PM100000'],
    ['Inheritance Tax (overview)', 'Standard IHT rate is 40% on estates above the nil rate band of £325,000 (frozen until at least April 2028). Residence nil rate band adds £175,000 for direct descendants. Agricultural Property Relief (APR) and Business Property Relief (BPR) can reduce or eliminate IHT on farming assets.', 'Applies to estates on death and certain lifetime transfers. APR/BPR are separate reliefs with own conditions.', 'IHT account (IHT400) due within 12 months of death. Tax due 6 months after end of month of death.', 'Interest on late payment. Penalties for incorrect returns.', 'IHTA 1984 / HMRC IHTM24000'],
  ];

  for (const [topic, rule, conditions, deadlines, penalties, ref] of taxRules) {
    db.run(
      `INSERT INTO tax_rules (topic, rule, conditions, deadlines, penalties, hmrc_ref, jurisdiction)
       VALUES (?, ?, ?, ?, ?, ?, 'GB')`,
      [topic, rule, conditions, deadlines, penalties, ref]
    );
  }

  // --- APR guidance ---
  const aprData = [
    ['Owner-occupier farming own land', 1, '100% APR available. Land must have been occupied by the owner for agricultural purposes for at least 2 years before transfer.', '2-year occupation test: owner must occupy the property for agricultural purposes', null, 'HMRC IHTM24030: owner-occupier relief at 100% after 2 years', 'IHTA 1984 s 116 / IHTM24030'],
    ['Tenanted land (post-1 Sep 1995)', 1, '100% APR available. Land let on a tenancy beginning on or after 1 September 1995 qualifies for 100% relief.', '7-year ownership test: owner must have owned the property for 7 years if not occupying it', null, 'Post-1995 tenancies qualify at 100% — ATA 1995 Farm Business Tenancies', 'IHTA 1984 s 116(2)(b) / IHTM24060'],
    ['Tenanted land (pre-1 Sep 1995)', 1, '50% APR only. Land let on a tenancy that began before 1 September 1995 (typically an AHA 1986 tenancy) qualifies for only 50% relief.', '7-year ownership test', null, '50% rate applies to pre-September 1995 AHA tenancies. Consider BPR top-up if landlord is actively farming.', 'IHTA 1984 s 116(2)(a) / IHTM24061'],
    ['Farmhouse', 1, 'APR available on the farmhouse if it is of a character appropriate to the agricultural property. HMRC applies a proportionality test — the farmhouse must not be disproportionately large relative to the farming operation.', 'Character appropriate test: HMRC compares the farmhouse to the farming operation size', null, 'If farmhouse is disproportionate (a mansion on a small farm), HMRC will restrict or deny APR. Lloyds TSB v IRC (2002) established character appropriate test.', 'IHTA 1984 s 115(2) / IHTM24050 / Lloyds TSB v IRC'],
    ['Farm cottages', 1, 'APR available on cottages occupied by farm workers for agricultural purposes.', 'Must be occupied by a person employed in farming the property', null, 'Worker must be employed in agriculture on the property, not unrelated activities.', 'IHTA 1984 s 115(2) / IHTM24054'],
    ['Land in hand (not farmed)', 0, 'NO APR if owner simply holds agricultural land without farming it or letting it for agriculture. APR requires either occupation for agriculture by owner, or letting for agricultural purposes.', 'Must satisfy either owner-occupier test or letting test', null, 'Simply owning agricultural land is not sufficient. Land must be in agricultural use.', 'IHTA 1984 s 117 / IHTM24000'],
    ['Clawback on sale/change of use', 1, 'If property receiving APR is sold or its use changes within 7 years of the transfer (death or lifetime gift), HMRC can claw back the relief.', '7-year clawback period from date of death or transfer', '7 years', 'Applies to both death and lifetime transfers. New owner must maintain agricultural occupation.', 'IHTA 1984 s 124A / IHTM24200'],
    ['Trusts holding farmland', 1, 'APR available to trusts if a beneficiary occupies the land for agricultural purposes, or if the trust lets the land for agriculture.', 'Beneficiary must occupy for agriculture OR land must be let for agriculture', null, 'Relevant for farm succession planning. Discretionary trusts can qualify.', 'IHTA 1984 s 115-124C / IHTM24070'],
    ['Company-owned farms', 1, 'APR applies to shares in a farming company if the land owned by the company is of a character appropriate to agricultural property. BPR may also apply.', 'Character appropriate test applies to the underlying property', null, 'Relief attaches to the shares, not the land directly. Both APR and BPR claims possible.', 'IHTA 1984 s 122 / IHTM24100'],
    ['Diversified-use land', 0, 'NO APR on the portion of land used for non-agricultural purposes: farm shop buildings, holiday lets, solar panel installations, equestrian use (non-agricultural). Only the agricultural portion qualifies.', 'APR applies only to agricultural property — non-agricultural use disqualifies that portion', null, 'Common trap: diversification income is good for cash flow but can reduce IHT relief. Careful apportionment needed between agricultural and non-agricultural value.', 'IHTA 1984 s 115(2) / IHTM24012'],
  ];

  for (const [scenario, relief, conditions, occupation, clawback, notes, ref] of aprData) {
    db.run(
      `INSERT INTO apr_guidance (scenario, relief_available, conditions, occupation_test, clawback_period, notes, hmrc_ref, jurisdiction)
       VALUES (?, ?, ?, ?, ?, ?, ?, 'GB')`,
      [scenario, relief, conditions, occupation, clawback, notes, ref]
    );
  }

  // --- Tenancy rules ---
  const tenancyData = [
    ['AHA_1986', 'Succession', 'Up to 2 successions allowed (3 total tenancies including original). Successor must be a close family member (spouse, child, sibling, etc.) and must demonstrate that their principal source of livelihood has been derived from agricultural work on the holding for 5 of the preceding 7 years.', 'Close family only. Livelihood test: principal livelihood from farming the holding for 5/7 years. Suitability test: must be capable of farming the holding.', 'AHA 1986 ss 36-48'],
    ['AHA_1986', 'Rent review', 'Rent reviews every 3 years. Either party can demand a review. Rent determined by productive capacity of the holding (not open market rent). Disputes resolved by RICS arbitration.', 'Rent based on productive capacity, not open market value. Either party can trigger a review.', 'AHA 1986 s 12'],
    ['AHA_1986', 'Compensation', 'Tenant entitled to compensation for improvements at end of tenancy, provided landlord consented (or tribunal approval obtained). Compensation based on increase in value of the holding.', 'Landlord consent required for most improvements (some short-term improvements exempt).', 'AHA 1986 ss 64-83'],
    ['AHA_1986', 'Security of tenure', 'Lifetime security of tenure. Very difficult for landlord to terminate — only for breach, non-agricultural use, or planning permission for non-agricultural development. Tenant protected even after retirement if successor available.', 'Termination only for specific statutory grounds. Cannot terminate simply because landlord wants land back.', 'AHA 1986 ss 25-27'],
    ['ATA_1995', 'Term', 'Fixed-term Farm Business Tenancy (FBT) of 2 or more years. No automatic succession rights. Tenancy ends at expiry of term unless parties agree to renew.', 'Minimum 2-year term for FBT. Shorter agreements possible but may not qualify as FBT.', 'ATA 1995 ss 1-8'],
    ['ATA_1995', 'Rent review', 'Rent review as per the tenancy agreement, typically every 3 years. Open market rent basis (unlike AHA productive capacity basis). Either party can refer to arbitration.', 'Open market rent, not productive capacity. Agreement terms govern review frequency.', 'ATA 1995 ss 9-14'],
    ['ATA_1995', 'Compensation', 'Tenant entitled to compensation for tenant improvements if landlord consented. Compensation for routine improvements as per agreement.', 'Landlord consent required. Amount based on increase in holding value.', 'ATA 1995 ss 15-27'],
    ['ATA_1995', 'Termination', 'Tenancy ends at expiry of fixed term. Landlord must give at least 12 months notice before term end. If no notice given, tenancy continues from year to year.', 'Minimum 12 months notice to quit. If neither party gives notice, rolls to periodic tenancy.', 'ATA 1995 s 5-7'],
    ['AHA_1986', 'Gladstone v Bower', 'Tenancy for less than 1 year and 364 days (i.e., less than 2 years) can avoid AHA 1986 succession rights. Commonly used by landlords to avoid creating full AHA tenancies. However, successive short lettings may be treated as a single AHA tenancy.', 'Must be genuinely short-term. Repeated renewals may create an AHA tenancy by estoppel.', 'Gladstone v Bower [1960] / AHA 1986 s 2'],
    ['AHA_1986', 'Tenant right', 'Outgoing tenant entitled to compensation for growing crops, cultivations, and unexpired manures/fertiliser applications at end of tenancy. This is the tenant right claim.', 'Valued at cost of inputs applied less benefit already obtained. Standard industry practice.', 'AHA 1986 s 65 / custom of the country'],
  ];

  for (const [type, topic, rule, conditions, section] of tenancyData) {
    db.run(
      `INSERT INTO tenancy_rules (tenancy_type, topic, rule, conditions, act_section, jurisdiction)
       VALUES (?, ?, ?, ?, ?, 'GB')`,
      [type, topic, rule, conditions, section]
    );
  }

  // --- Diversification ---
  const diversData = [
    ['Agricultural building to dwellings (Class Q)', 'Class Q', 865, 'New dwelling qualifies for council tax, not business rates', 'Convert agricultural building to up to 5 dwellings (max 865 m2 total floor area). Permitted development — prior approval required from LPA for transport, contamination, flooding, noise, design. Building must have been in agricultural use on 20 March 2013 or last used as such. Structural works limited — building must be capable of conversion without substantial rebuild.'],
    ['Agricultural building to retail/farm shop (Class R)', 'Class R', 500, 'Business rates apply to commercial use', 'Convert agricultural building to retail (farm shop), storage, or distribution use. Max 500 m2 floor area. Permitted development — 28-day prior notification to LPA required. Building must be on an agricultural unit of at least 5 hectares.'],
    ['Agricultural building to school/nursery (Class S)', 'Class S', 500, 'Exempt from business rates if state-funded', 'Convert agricultural building to state-funded school or registered nursery. Max 500 m2 floor area. Permitted development with prior approval.'],
    ['Farm shop (new build)', null, null, 'Business rates apply; may qualify for small business rate relief', 'New-build farm shop requires full planning permission from LPA if not using permitted development Class R conversion. Must comply with local plan policies on retail in rural areas.'],
    ['Camping and glamping', null, null, 'May qualify as small business for rates; certificated site status avoids full licence', '28-day camping exemption per calendar year per field without planning permission (Caravan Sites and Control of Development Act 1960). Beyond 28 days requires planning permission and/or caravan site licence. Certificated site status from Camping and Caravanning Club allows up to 5 units without full site licence.'],
    ['Renewable energy (solar farm)', null, null, 'Business rates apply to solar installations; may offset against farm income', 'Solar farms under 50 MW determined by local planning authority (LPA). Over 50 MW is a Nationally Significant Infrastructure Project (NSIP) requiring Development Consent Order. DEFRA prefers agricultural land class 3b or worse for solar — use of Best and Most Versatile land (class 1, 2, 3a) is discouraged.'],
  ];

  for (const [activity, pdClass, maxArea, ratesImpact, notes] of diversData) {
    db.run(
      `INSERT INTO diversification (activity, pd_class, max_floor_area_m2, business_rates_impact, planning_notes, jurisdiction)
       VALUES (?, ?, ?, ?, ?, 'GB')`,
      [activity, pdClass, maxArea, ratesImpact, notes]
    );
  }

  // --- FTS5 search index ---
  const ftsEntries = [
    ['Winter wheat after oilseed rape rotation', 'Winter wheat following oilseed rape is a standard UK rotation. Break crop benefit improves yield. OSR provides different disease spectrum reducing take-all risk in wheat.', 'rotation'],
    ['Second wheat yield penalty', 'Second wheat (winter wheat after winter wheat) incurs a 10-15% yield penalty. Blackgrass risk is HIGH in continuous cereal rotations. Take-all disease builds up.', 'rotation'],
    ['Oilseed rape 4-year break clubroot', 'Oilseed rape requires minimum 4-year break between OSR crops to manage clubroot and sclerotinia. OSR after OSR should be avoided.', 'rotation'],
    ['Winter wheat gross margin 2024/25', 'Winter wheat gross margin 2024/25: output £1,530/ha, variable costs £650/ha, gross margin £880/ha. Top quartile £1,100/ha, bottom quartile £600/ha.', 'margins'],
    ['Dairy cow gross margin', 'Dairy cow gross margin 2024/25: output £2,800/head, variable costs £1,900/head, GM £900/head. Top quartile £1,300/head.', 'margins'],
    ['Making Tax Digital MTD income tax', 'MTD for Income Tax mandatory from April 2026 for income over £50,000. Quarterly updates via compatible software. Threshold drops to £30,000 from April 2027.', 'tax'],
    ['Farmers averaging profits 2 or 5 years', 'Farmers can average profits over 2 or 5 years. Claim within first anniversary of filing deadline. Smooths volatile farming income for income tax.', 'tax'],
    ['Capital allowances AIA plant machinery', 'Annual Investment Allowance £1,000,000 for qualifying plant and machinery. 100% first-year allowance. Structures and Buildings Allowance 3% over 33.33 years for farm buildings.', 'tax'],
    ['Agricultural Property Relief APR inheritance tax', 'APR provides 100% or 50% relief from IHT on agricultural property. Owner-occupier: 100% after 2 years. Tenanted pre-1995: 50%. Post-1995 FBT: 100%. Farmhouse must be character appropriate.', 'apr'],
    ['APR farmhouse character appropriate test', 'HMRC tests whether farmhouse is proportionate to the farming operation. Lloyds TSB v IRC (2002) established the character appropriate test. Mansion on small farm will fail.', 'apr'],
    ['APR diversified land no relief', 'No APR on diversified-use land: farm shop buildings, holiday lets, solar panels, equestrian use. Only the agricultural portion qualifies for relief.', 'apr'],
    ['AHA 1986 succession rights', 'AHA 1986 tenancies allow up to 2 successions. Close family only. Livelihood test: principal livelihood from farming the holding for 5 of preceding 7 years.', 'tenancy'],
    ['ATA 1995 FBT fixed term no succession', 'Farm Business Tenancies under ATA 1995: fixed term (2+ years), no automatic succession rights, open market rent reviews, 12+ months notice to terminate.', 'tenancy'],
    ['Gladstone v Bower short tenancy', 'Gladstone v Bower: tenancy for less than 2 years avoids AHA succession. Used by landlords to prevent creating full AHA tenancies. Repeated renewals risky.', 'tenancy'],
    ['Class Q permitted development dwelling conversion', 'Class Q: convert agricultural building to up to 5 dwellings (max 865 m2). Permitted development with prior approval. Building must be capable of conversion without substantial rebuild.', 'diversification'],
    ['Class R farm shop conversion', 'Class R: convert agricultural building to retail/farm shop (max 500 m2). Permitted development. 28-day prior notification. Unit must be at least 5 hectares.', 'diversification'],
    ['Camping glamping 28-day exemption', '28-day camping exemption per field per year without planning permission. Beyond 28 days needs planning permission or caravan site licence.', 'diversification'],
    ['Solar farm planning agricultural land classification', 'Solar farms under 50 MW to LPA. Over 50 MW is NSIP. DEFRA prefers class 3b or worse agricultural land. Best and Most Versatile land (1, 2, 3a) discouraged.', 'diversification'],
    ['Flat rate VAT agriculture', 'Agricultural flat rate scheme: 4% flat rate addition. General flat rate 6.5% for agriculture. Cannot reclaim input VAT under flat rate scheme.', 'tax'],
    ['Partnership tax farming self assessment', 'Each partner files self-assessment. Profit allocation per partnership agreement. Partnership return SA800 filed separately.', 'tax'],
    ['Inheritance tax nil rate band 325000', 'IHT at 40% above nil rate band of £325,000 (frozen to April 2028). Residence nil rate band £175,000 for direct descendants. APR and BPR can reduce farming IHT.', 'tax'],
    ['Potatoes 5-year break PCN blight', 'Potatoes require minimum 5-year break between crops to manage Potato Cyst Nematode (PCN) and blight. High input costs but high gross margins.', 'rotation'],
    ['Blackgrass risk continuous cereals', 'Blackgrass risk increases with continuous cereal cropping. Spring cropping is the most effective break. Oilseed rape is moderate risk. Beans and sugar beet are low risk.', 'rotation'],
  ];

  for (const [title, body, topic] of ftsEntries) {
    db.run(
      `INSERT INTO search_index (title, body, topic, jurisdiction) VALUES (?, ?, ?, 'GB')`,
      [title, body, topic]
    );
  }

  // Metadata
  db.run("INSERT OR REPLACE INTO db_metadata (key, value) VALUES ('last_ingest', '2026-04-03')", []);
  db.run("INSERT OR REPLACE INTO db_metadata (key, value) VALUES ('build_date', '2026-04-03')", []);

  return db;
}
