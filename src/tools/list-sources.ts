import { buildMeta } from '../metadata.js';
import type { Database } from '../db.js';

interface Source {
  name: string;
  authority: string;
  official_url: string;
  retrieval_method: string;
  update_frequency: string;
  license: string;
  coverage: string;
  last_retrieved?: string;
}

export function handleListSources(db: Database): { sources: Source[]; _meta: ReturnType<typeof buildMeta> } {
  const lastIngest = db.get<{ value: string }>('SELECT value FROM db_metadata WHERE key = ?', ['last_ingest']);

  const sources: Source[] = [
    {
      name: 'AHDB Farmbench / Pocketbook',
      authority: 'Agriculture and Horticulture Development Board',
      official_url: 'https://ahdb.org.uk/farmbench',
      retrieval_method: 'MANUAL_EXTRACT',
      update_frequency: 'annual',
      license: 'Open Government Licence v3',
      coverage: 'Gross margins for arable and livestock enterprises, benchmarking data',
      last_retrieved: lastIngest?.value,
    },
    {
      name: 'HMRC Manuals and Guidance',
      authority: 'HM Revenue & Customs',
      official_url: 'https://www.gov.uk/hmrc-internal-manuals',
      retrieval_method: 'MANUAL_EXTRACT',
      update_frequency: 'ongoing',
      license: 'Open Government Licence v3',
      coverage: 'Agricultural tax rules, APR guidance, MTD requirements, capital allowances',
      last_retrieved: lastIngest?.value,
    },
    {
      name: 'UK Legislation (AHA 1986 / ATA 1995)',
      authority: 'The National Archives',
      official_url: 'https://www.legislation.gov.uk',
      retrieval_method: 'MANUAL_EXTRACT',
      update_frequency: 'as_amended',
      license: 'Open Government Licence v3',
      coverage: 'Agricultural tenancy rules, succession rights, rent review, compensation',
      last_retrieved: lastIngest?.value,
    },
    {
      name: 'Town and Country Planning (GPDO) 2015',
      authority: 'Ministry of Housing, Communities and Local Government',
      official_url: 'https://www.legislation.gov.uk/uksi/2015/596',
      retrieval_method: 'MANUAL_EXTRACT',
      update_frequency: 'as_amended',
      license: 'Open Government Licence v3',
      coverage: 'Permitted development rights for agricultural diversification (Class Q, R, S)',
      last_retrieved: lastIngest?.value,
    },
  ];

  return {
    sources,
    _meta: buildMeta({ source_url: 'https://ahdb.org.uk/farm-business' }),
  };
}
