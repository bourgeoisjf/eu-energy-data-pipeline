"""
List of countries and their specific ENTSO-E bidding zones for Generation and Price.
Separating these zones ensures data completeness for countries like DE and IT.
"""

COUNTRIES = {
    "FR": {
        "country_name": "France",
        "gen_zone": "10YFR-RTE------C",
        "price_zone": "10YFR-RTE------C",
    },
    "DE": {
        "country_name": "Germany",
        "gen_zone": "10Y1001A1001A83F",  # DE-LU-AT (Geração)
        "price_zone": "10Y1001A1001A82H",  # DE-LU (Preço Day-Ahead)
    },
    "ES": {
        "country_name": "Spain",
        "gen_zone": "10YES-REE------0",
        "price_zone": "10YES-REE------0",
    },
    "IT": {
        "country_name": "Italy",
        "gen_zone": "10YIT-GRTN-----B",  # Italy Total (Geração)
        "price_zone": "10Y1001A1001A73I",  # Italy North (Referência de Preço)
    },
    "PT": {
        "country_name": "Portugal",
        "gen_zone": "10YPT-REN------W",
        "price_zone": "10YPT-REN------W",
    },
    "NL": {
        "country_name": "Netherlands",
        "gen_zone": "10YNL----------L",
        "price_zone": "10YNL----------L",
    },
    "BE": {
        "country_name": "Belgium",
        "gen_zone": "10YBE----------2",
        "price_zone": "10YBE----------2",
    },
    "AT": {
        "country_name": "Austria",
        "gen_zone": "10YAT-APG------L",
        "price_zone": "10YAT-APG------L",
    },
    "CH": {
        "country_name": "Switzerland",
        "gen_zone": "10YCH-SWISSGRIDZ",
        "price_zone": "10YCH-SWISSGRIDZ",
    },
    "PL": {
        "country_name": "Poland",
        "gen_zone": "10YPL-PSE------S",
        "price_zone": "10YPL-PSE------S",
    },
    "GR": {
        "country_name": "Greece",
        "gen_zone": "10YGR-HTSO-----Y",
        "price_zone": "10YGR-HTSO-----Y",
    },
}
