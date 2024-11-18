import hashlib
import pandas as pd
import numpy as np
from collections import Counter

import os
import sys
sys.path.append(os.path.abspath(os.path.join('..')))


def generate_hash(row, columns):
    combined_string = "_".join(str(row[col]) for col in columns)
    return hashlib.md5(combined_string.encode()).hexdigest()


s3_bucket = "s3://e2e-shop-bucket/"

## Input Paths
input_raw_retail = f"{s3_bucket}raw/retail.csv"
input_raw_main_product_descriptions = f"{s3_bucket}raw/main_product_descriptions.csv"

# Output paths
output_clean = f"{s3_bucket}clean/"


raw_retail = pd.read_csv(input_raw_retail, encoding='ISO-8859-1')
raw_main_product_descriptions = pd.read_csv(input_raw_main_product_descriptions)

### dim_item_family
item_family = raw_main_product_descriptions.copy()
item_family = item_family.rename(columns={
    'item_family': 'item_family_id',
    'category': 'item_category'
})

item_family['item_family_id'] = item_family['item_family_id'].astype('Int64')
item_family['item_family_description'] = item_family['item_family_description'].astype(str)
item_family['item_category'] = item_family['item_category'].astype(str)

dim_item_family = item_family.copy()


### dim_date
date_range = pd.date_range(start='2009-01-01', end='2025-12-31', freq='D')

dim_date = pd.DataFrame({
    'date_id': range(1, len(date_range) + 1),
    'date': date_range.date,
    'year': date_range.year,
    'month': date_range.month,
    'day': date_range.day,
    'day_of_week': date_range.strftime('%A'),
    'is_weekend': date_range.weekday >= 5 
})


### dim_location
country_data = {
    'country_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 
                   21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 
                   38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 
                   55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 
                   72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 
                   89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 
                   105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 
                   118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 
                   131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 
                   144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 
                   157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 
                   170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 
                   183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 
                   196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 
                   209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 
                   222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 
                   235, 236, 237, 238, 239, 240, 241, 242, 243, 244],
    'iso_country_code': ['AF', 'AL', 'DZ', 'AS', 'AD', 'AO', 'AI', 'AQ', 'AG', 'AR', 'AM', 
                         'AW', 'AU', 'AT', 'AZ', 'BS', 'BH', 'BD', 'BB', 'BY', 'BE', 'BZ', 
                         'BJ', 'BM', 'BT', 'BO', 'BA', 'BW', 'BV', 'BR', 'IO', 'BN', 'BG', 
                         'BF', 'BI', 'KH', 'CM', 'CA', 'CV', 'KY', 'CF', 'TD', 'CL', 'CN', 
                         'CX', 'CC', 'CO', 'KM', 'CG', 'CD', 'CK', 'CR', 'CI', 'HR', 'CU', 
                         'CY', 'CZ', 'DK', 'DJ', 'DM', 'DO', 'EC', 'EG', 'SV', 'GQ', 'ER', 
                         'EE', 'ET', 'FK', 'FO', 'FJ', 'FI', 'FR', 'GF', 'PF', 'TF', 'GA', 
                         'GM', 'GE', 'DE', 'GH', 'GI', 'GR', 'GL', 'GD', 'GP', 'GU', 'GT', 
                         'GN', 'GW', 'GY', 'HT', 'HM', 'VA', 'HN', 'HK', 'HU', 'IS', 'IN', 
                         'ID', 'IR', 'IQ', 'IE', 'IL', 'IT', 'JM', 'JP', 'JO', 'KZ', 'KE', 
                         'KI', 'KP', 'KR', 'KW', 'KG', 'LA', 'LV', 'LB', 'LS', 'LR', 'LY', 
                         'LI', 'LT', 'LU', 'MO', 'MK', 'MG', 'MW', 'MY', 'MV', 'ML', 'MT', 
                         'MH', 'MQ', 'MR', 'MU', 'YT', 'MX', 'FM', 'MD', 'MC', 'MN', 'MS', 
                         'MA', 'MZ', 'MM', 'NA', 'NR', 'NP', 'NL', 'AN', 'NC', 'NZ', 'NI', 
                         'NE', 'NG', 'NU', 'NF', 'MP', 'NO', 'OM', 'PK', 'PW', 'PS', 'PA', 
                         'PG', 'PY', 'PE', 'PH', 'PN', 'PL', 'PT', 'PR', 'QA', 'RE', 'RO', 
                         'RU', 'RW', 'SH', 'KN', 'LC', 'PM', 'VC', 'WS', 'SM', 'ST', 'SA', 
                         'SN', 'CS', 'SC', 'SL', 'SG', 'SK', 'SI', 'SB', 'SO', 'ZA', 'GS', 
                         'ES', 'LK', 'SD', 'SR', 'SJ', 'SZ', 'SE', 'CH', 'SY', 'TW', 'TJ', 
                         'TZ', 'TH', 'TL', 'TG', 'TK', 'TO', 'TT', 'TN', 'TR', 'TM', 'TC', 
                         'TV', 'UG', 'UA', 'AE', 'GB', 'US', 'UM', 'UY', 'UZ', 'VU', 'VE', 
                         'VN', 'VG', 'VI', 'WF', 'EH', 'YE', 'ZM', 'ZW', 'IE', 'XX', '00','EU', 'ZA'],
    'country_name': ['Afghanistan', 'Albania', 'Algeria', 'American Samoa', 'Andorra', 
                     'Angola', 'Anguilla', 'Antarctica', 'Antigua and Barbuda', 'Argentina', 
                     'Armenia', 'Aruba', 'Australia', 'Austria', 'Azerbaijan', 'Bahamas', 
                     'Bahrain', 'Bangladesh', 'Barbados', 'Belarus', 'Belgium', 'Belize', 
                     'Benin', 'Bermuda', 'Bhutan', 'Bolivia', 'Bosnia and Herzegovina', 
                     'Botswana', 'Bouvet Island', 'Brazil', 'British Indian Ocean Territory', 
                     'Brunei Darussalam', 'Bulgaria', 'Burkina Faso', 'Burundi', 'Cambodia', 
                     'Cameroon', 'Canada', 'Cape Verde', 'Cayman Islands', 'Central African Republic', 
                     'Chad', 'Chile', 'China', 'Christmas Island', 'Cocos (Keeling) Islands', 
                     'Colombia', 'Comoros', 'Congo', 'Congo, the Democratic Republic of the', 
                     'Cook Islands', 'Costa Rica', "Cote d'Ivoire", 'Croatia', 'Cuba', 'Cyprus', 
                     'Czech Republic', 'Denmark', 'Djibouti', 'Dominica', 'Dominican Republic', 
                     'Ecuador', 'Egypt', 'El Salvador', 'Equatorial Guinea', 'Eritrea', 'Estonia', 
                     'Ethiopia', 'Falkland Islands (Malvinas)', 'Faroe Islands', 'Fiji', 'Finland', 
                     'France', 'French Guiana', 'French Polynesia', 'French Southern Territories', 
                     'Gabon', 'Gambia', 'Georgia', 'Germany', 'Ghana', 'Gibraltar', 'Greece', 
                     'Greenland', 'Grenada', 'Guadeloupe', 'Guam', 'Guatemala', 'Guinea', 
                     'Guinea-Bissau', 'Guyana', 'Haiti', 'Heard Island and McDonald Islands', 
                     'Holy See (Vatican City State)', 'Honduras', 'Hong Kong', 'Hungary', 'Iceland', 
                     'India', 'Indonesia', 'Iran', 'Iraq', 'Ireland', 'Israel', 'Italy', 
                     'Jamaica', 'Japan', 'Jordan', 'Kazakhstan', 'Kenya', 'Kiribati', 
                     'Korea, Democratic People’s Republic of', 'Korea, Republic of', 'Kuwait', 
                     'Kyrgyzstan', 'Lao People’s Democratic Republic', 'Latvia', 'Lebanon', 'Lesotho', 
                     'Liberia', 'Libya', 'Liechtenstein', 'Lithuania', 'Luxembourg', 'Macao', 
                     'North Macedonia', 'Madagascar', 'Malawi', 'Malaysia', 'Maldives', 'Mali', 
                     'Malta', 'Marshall Islands', 'Martinique', 'Mauritania', 'Mauritius', 'Mayotte', 
                     'Mexico', 'Micronesia (Federated States of)', 'Moldova', 'Monaco', 'Mongolia', 
                     'Montserrat', 'Morocco', 'Mozambique', 'Myanmar', 'Namibia', 'Nauru', 'Nepal', 
                     'Netherlands', 'Netherlands Antilles', 'New Caledonia', 'New Zealand', 'Nicaragua', 
                     'Niger', 'Nigeria', 'Niue', 'Norfolk Island', 'Northern Mariana Islands', 
                     'Norway', 'Oman', 'Pakistan', 'Palau', 'Palestine, State of', 'Panama', 
                     'Papua New Guinea', 'Paraguay', 'Peru', 'Philippines', 'Pitcairn', 'Poland', 
                     'Portugal', 'Puerto Rico', 'Qatar', 'Reunion', 'Romania', 'Russian Federation', 
                     'Rwanda', 'Saint Helena', 'Saint Kitts and Nevis', 'Saint Lucia', 
                     'Saint Pierre and Miquelon', 'Saint Vincent and the Grenadines', 'Samoa', 
                     'San Marino', 'Sao Tome and Principe', 'Saudi Arabia', 'Senegal', 
                     'Serbia and Montenegro', 'Seychelles', 'Sierra Leone', 'Singapore', 'Slovakia', 
                     'Slovenia', 'Solomon Islands', 'Somalia', 'South Africa', 
                     'South Georgia and the South Sandwich Islands', 'Spain', 'Sri Lanka', 'Sudan', 
                     'Suriname', 'Svalbard and Jan Mayen', 'Eswatini', 'Sweden', 'Switzerland', 
                     'Syrian Arab Republic', 'Taiwan', 'Tajikistan', 'Tanzania', 'Thailand', 
                     'Timor-Leste', 'Togo', 'Tokelau', 'Tonga', 'Trinidad and Tobago', 
                     'Tunisia', 'Turkey', 'Turkmenistan', 'Turks and Caicos Islands', 'Tuvalu', 
                     'Uganda', 'Ukraine', 'United Arab Emirates', 'United Kingdom', 'USA', 
                     'United States Minor Outlying Islands', 'Uruguay', 'Uzbekistan', 'Vanuatu', 
                     'Venezuela', 'Vietnam', 'British Virgin Islands', 'U.S. Virgin Islands', 
                     'Wallis and Futuna', 'Western Sahara', 'Yemen', 'Zambia', 'Zimbabwe',
                     'EIRE', 'Channel Islands','Unspecified','European Community','RSA'],
    'country_code_name': ['AFGHANISTAN', 'ALBANIA', 'ALGERIA', 'AMERICAN SAMOA', 'ANDORRA', 
                          'ANGOLA', 'ANGUILLA', 'ANTARCTICA', 'ANTIGUA AND BARBUDA', 'ARGENTINA', 
                          'ARMENIA', 'ARUBA', 'AUSTRALIA', 'AUSTRIA', 'AZERBAIJAN', 'BAHAMAS', 
                          'BAHRAIN', 'BANGLADESH', 'BARBADOS', 'BELARUS', 'BELGIUM', 'BELIZE', 
                          'BENIN', 'BERMUDA', 'BHUTAN', 'BOLIVIA', 'BOSNIA AND HERZEGOVINA', 
                          'BOTSWANA', 'BOUVET ISLAND', 'BRAZIL', 'BRITISH INDIAN OCEAN TERRITORY', 
                          'BRUNEI DARUSSALAM', 'BULGARIA', 'BURKINA FASO', 'BURUNDI', 'CAMBODIA', 
                          'CAMEROON', 'CANADA', 'CAPE VERDE', 'CAYMAN ISLANDS', 'CENTRAL AFRICAN REPUBLIC', 
                          'CHAD', 'CHILE', 'CHINA', 'CHRISTMAS ISLAND', 'COCOS (KEELING) ISLANDS', 
                          'COLOMBIA', 'COMOROS', 'CONGO', 'CONGO, THE DEMOCRATIC REPUBLIC OF THE', 
                          'COOK ISLANDS', 'COSTA RICA', "COTE D'IVOIRE", 'CROATIA', 'CUBA', 'CYPRUS', 
                          'CZECH REPUBLIC', 'DENMARK', 'DJIBOUTI', 'DOMINICA', 'DOMINICAN REPUBLIC', 
                          'ECUADOR', 'EGYPT', 'EL SALVADOR', 'EQUATORIAL GUINEA', 'ERITREA', 'ESTONIA', 
                          'ETHIOPIA', 'FALKLAND ISLANDS (MALVINAS)', 'FAROE ISLANDS', 'FIJI', 'FINLAND', 
                          'FRANCE', 'FRENCH GUIANA', 'FRENCH POLYNESIA', 'FRENCH SOUTHERN TERRITORIES', 
                          'GABON', 'GAMBIA', 'GEORGIA', 'GERMANY', 'GHANA', 'GIBRALTAR', 'GREECE', 
                          'GREENLAND', 'GRENADA', 'GUADELOUPE', 'GUAM', 'GUATEMALA', 'GUINEA', 
                          'GUINEA-BISSAU', 'GUYANA', 'HAITI', 'HEARD ISLAND AND MCDONALD ISLANDS', 
                          'HOLY SEE (VATICAN CITY STATE)', 'HONDURAS', 'HONG KONG', 'HUNGARY', 'ICELAND', 
                          'INDIA', 'INDONESIA', 'IRAN, ISLAMIC REPUBLIC OF', 'IRAQ', 'IRELAND', 'ISRAEL', 
                          'ITALY', 'JAMAICA', 'JAPAN', 'JORDAN', 'KAZAKHSTAN', 'KENYA', 'KIRIBATI', 
                          'KOREA, DEMOCRATIC PEOPLE’S REPUBLIC OF', 'KOREA, REPUBLIC OF', 'KUWAIT', 
                          'KYRGYZSTAN', "LAO PEOPLE’S DEMOCRATIC REPUBLIC", 'LATVIA', 'LEBANON', 'LESOTHO', 
                          'LIBERIA', 'LIBYAN ARAB JAMAHIRIYA', 'LIECHTENSTEIN', 'LITHUANIA', 'LUXEMBOURG', 
                          'MACAO', 'MACEDONIA, THE FORMER YUGOSLAV REPUBLIC OF', 'MADAGASCAR', 'MALAWI', 
                          'MALAYSIA', 'MALDIVES', 'MALI', 'MALTA', 'MARSHALL ISLANDS', 'MARTINIQUE', 
                          'MAURITANIA', 'MAURITIUS', 'MAYOTTE', 'MEXICO', 'MICRONESIA, FEDERATED STATES OF', 
                          'MOLDOVA, REPUBLIC OF', 'MONACO', 'MONGOLIA', 'MONTSERRAT', 'MOROCCO', 
                          'MOZAMBIQUE', 'MYANMAR', 'NAMIBIA', 'NAURU', 'NEPAL', 'NETHERLANDS', 
                          'NETHERLANDS ANTILLES', 'NEW CALEDONIA', 'NEW ZEALAND', 'NICARAGUA', 'NIGER', 
                          'NIGERIA', 'NIUE', 'NORFOLK ISLAND', 'NORTHERN MARIANA ISLANDS', 'NORWAY', 
                          'OMAN', 'PAKISTAN', 'PALAU', 'PALESTINIAN TERRITORY, OCCUPIED', 'PANAMA', 
                          'PAPUA NEW GUINEA', 'PARAGUAY', 'PERU', 'PHILIPPINES', 'PITCAIRN', 'POLAND', 
                          'PORTUGAL', 'PUERTO RICO', 'QATAR', 'REUNION', 'ROMANIA', 'RUSSIAN FEDERATION', 
                          'RWANDA', 'SAINT HELENA', 'SAINT KITTS AND NEVIS', 'SAINT LUCIA', 
                          'SAINT PIERRE AND MIQUELON', 'SAINT VINCENT AND THE GRENADINES', 'SAMOA', 
                          'SAN MARINO', 'SAO TOME AND PRINCIPE', 'SAUDI ARABIA', 'SENEGAL', 
                          'SERBIA AND MONTENEGRO', 'SEYCHELLES', 'SIERRA LEONE', 'SINGAPORE', 'SLOVAKIA', 
                          'SLOVENIA', 'SOLOMON ISLANDS', 'SOMALIA', 'SOUTH AFRICA', 
                          'SOUTH GEORGIA AND THE SOUTH SANDWICH ISLANDS', 'SPAIN', 'SRI LANKA', 'SUDAN', 
                          'SURINAME', 'SVALBARD AND JAN MAYEN', 'SWAZILAND', 'SWEDEN', 'SWITZERLAND', 
                          'SYRIAN ARAB REPUBLIC', 'TAIWAN, PROVINCE OF CHINA', 'TAJIKISTAN', 'TANZANIA', 
                          'THAILAND', 'TIMOR-LESTE', 'TOGO', 'TOKELAU', 'TONGA', 'TRINIDAD AND TOBAGO', 
                          'TUNISIA', 'TURKEY', 'TURKMENISTAN', 'TURKS AND CAICOS ISLANDS', 'TUVALU', 'UGANDA', 
                          'UKRAINE', 'UNITED ARAB EMIRATES', 'UNITED KINGDOM', 'UNITED STATES', 
                          'UNITED STATES MINOR OUTLYING ISLANDS', 'URUGUAY', 'UZBEKISTAN', 'VANUATU', 
                          'VENEZUELA', 'VIET NAM', 'VIRGIN ISLANDS, BRITISH', 'VIRGIN ISLANDS, U.S.', 
                          'WALLIS AND FUTUNA', 'WESTERN SAHARA', 'YEMEN', 'ZAMBIA', 'ZIMBABWE',
                          'EIRE','CHANNEL ISLANDS', 'UNSPECIFIED', 'EU', 'RSA'],
}
dim_location = pd.DataFrame(country_data)

dim_location['country_id'] = dim_location['country_id'].astype('Int64')
dim_location['iso_country_code'] = dim_location['iso_country_code'].astype(str)
dim_location['country_name'] = dim_location['country_name'].astype(str)
dim_location['country_code_name'] = dim_location['country_code_name'].astype(str)

dim_location = dim_location[['country_id','iso_country_code', 'country_name', 'country_code_name']]


### fact_transactions

transactions = raw_retail.copy()

transactions = transactions.rename(columns={
    'InvoiceNo': 'invoice_id',
    'StockCode': 'item_id',
    'Description': 'item_description',
    'Quantity': 'quantity_amount',
    'InvoiceDate': 'event_timestamp_invoiced_at',
    'UnitPrice': 'unit_price_eur',
    'CustomerID': 'customer_id',
    'Country': 'country_name'
})

transactions['invoice_id'] = transactions['invoice_id'].astype(str)
transactions['item_id'] = transactions['item_id'].astype(str)
transactions['item_description'] = transactions['item_description'].astype(str)
transactions['quantity_amount'] = transactions['quantity_amount'].astype(int)
transactions['event_timestamp_invoiced_at'] = pd.to_datetime(transactions['event_timestamp_invoiced_at'])
transactions['unit_price_eur'] = transactions['unit_price_eur'].astype(float)
transactions['customer_id'] = transactions['customer_id'].astype('Int64')
transactions['country_name'] = transactions['country_name'].astype(str)
transactions.index.name = 'transaction_id'
transactions.reset_index(inplace=True)
transactions['transaction_id'] = transactions['transaction_id'].astype('Int64')
transactions['item_uuid'] = transactions.apply(lambda row: generate_hash(row, ['item_id', 'item_description']), axis=1)

fact_transactions = transactions.copy()
fact_transactions['transaction_date_id'] = fact_transactions['event_timestamp_invoiced_at'].dt.date.map(
    dim_date.set_index('date')['date_id']
)
fact_transactions['country_id'] = fact_transactions['country_name'].map(
    dim_location.set_index('country_name')['country_id']
)
fact_transactions['date'] = fact_transactions['event_timestamp_invoiced_at'].dt.date
fact_transactions['date_id'] = fact_transactions['date'].map(
    dim_date.set_index('date')['date_id']
)
fact_transactions['total_price_eur'] = fact_transactions['quantity_amount'] * fact_transactions['unit_price_eur']
fact_transactions = fact_transactions[['transaction_id', 'invoice_id', 'event_timestamp_invoiced_at', 'date_id', 'item_uuid', 'item_id', 'transaction_date_id',
                                       'quantity_amount', 'unit_price_eur', 'total_price_eur', 'customer_id', 
                                       'country_id']]

### dim_customers
from faker import Faker
import random
import pandas as pd

customers = transactions[['customer_id', 'country_name']].drop_duplicates().dropna().reset_index(drop=True)

faker = Faker()
countries_locale = {
    "Australia": 'en_AU', "Austria": 'de_AT', "Bahrain": 'en_US', "Belgium": 'fr_BE',
    "Brazil": 'pt_BR', "Canada": 'en_CA', "Channel Islands": 'en_GB', "Cyprus": 'el_CY',
    "Czech Republic": 'cs_CZ', "Denmark": 'da_DK', "EIRE": 'en_IE', "European Community": 'en_US',
    "Finland": 'fi_FI', "France": 'fr_FR', "Germany": 'de_DE', "Greece": 'el_GR',
    "Iceland": 'en_US', "Israel": 'he_IL', "Italy": 'it_IT', "Japan": 'ja_JP',
    "Lebanon": 'ar_SA', "Lithuania": 'lt_LT', "Malta": 'mt_MT', "Netherlands": 'nl_NL',
    "Norway": 'no_NO', "Poland": 'pl_PL', "Portugal": 'pt_PT', "RSA": 'en_US',
    "Saudi Arabia": 'ar_SA', "Singapore": 'en_US', "Spain": 'es_ES', "Sweden": 'sv_SE',
    "Switzerland": 'de_CH', "USA": 'en_US', "United Arab Emirates": 'ar_AE',
    "United Kingdom": 'en_GB', "Unspecified": 'en_US'
}

names_by_country = {}
for country, locale in countries_locale.items():
    faker = Faker(locale)
    first_names = [faker.first_name() for _ in range(1000)]
    last_names = [faker.last_name() for _ in range(1000)]
    full_names = [f"{random.choice(first_names)} {random.choice(last_names)}" for _ in range(1000)]
    names_by_country[country] = full_names

def assign_name(row):
    country = row['country_name']
    if country in names_by_country:
        return random.choice(names_by_country[country])
    return "Unknown"

customers['customer_name'] = customers.apply(assign_name, axis=1)
customers = customers.drop(columns=['country_name'])
dim_customers = customers.drop_duplicates(subset=['customer_id'], keep='first').reset_index(drop=True)


### dim_items

items = transactions.groupby(['item_uuid','item_id', 'item_description']).count()[[]].reset_index()

items['is_operational_item'] = (items['item_id'].str.len() < 5) | (items['item_id'].str.contains('gift', case=False))
items['item_family_id'] = items['item_id'].str.extract(r'^(\d+)', expand=False)
items['item_variant'] = items['item_id'].str.extract(r'(\D+)$', expand=False).fillna('')
items['item_variant'] = np.where(items['item_id'] == items['item_family_id'],  np.nan, items['item_variant'])

selected_items = items[items['is_operational_item'] == False]

selected_items['is_all_uppercase'] = selected_items['item_description'].str.isupper()
selected_items['description_len'] = selected_items['item_description'].str.len()
selected_items = selected_items.sort_values(by=['item_id', 'description_len'], ascending=[True, False])
selected_items['description_order'] = selected_items.groupby('item_id').cumcount() + 1

weird_items = selected_items[((selected_items['description_order'] > 1) & (selected_items['description_len'] < 13)) | ~selected_items['is_all_uppercase']]
description_list = weird_items['item_description'].drop_duplicates().tolist()
print(description_list)

is_unknown_item = []
is_error_item = []
is_special_item = []
is_modification_item = []

for description in description_list:
    description_lower = description.lower()
    if any(word in description_lower for word in [
        'check', 'damage', 'wet', 'lost', 'found', 'error', 'wrong', 'faulty', 'mouldy', 'smashed', 'broken', 'crush', 'crack', 'unsaleable', 'missing', 'throw away'
    ]):
        is_error_item.append(description)
    elif any(word in description_lower for word in [
        'dotcom', 'amazon', 'fba', 'ebay', 'john lewis', 'showroom', 'voucher', 'cordial jug', 'website'
    ]):
        is_special_item.append(description)
    elif any(word in description_lower for word in [
        'adjust', 'sample', 'sold as', 'mailout', 'allocate', 'temp', 'credit', 'sale', 're-adjustment', 'label mix up'
    ]):
        is_modification_item.append(description)


is_unknown_item = ['?','??','???','Incorrect stock entry.',"can't find",'nan',None]

is_error_item += [
    'on cargo order', 'test', 'barcode problem', 'mixed up', 'michel oops', 'printing smudges/thrown away', 
    'had been put aside', 'rusty thrown away', 'incorrectly made-thrown away.', 'Breakages', 'counted', 
    'Had been put aside.', 'returned', 'thrown away', 'mystery! Only ever imported 1800', 'Dagamed', 
    'code mix up? 84930', 'Printing smudges/thrown away', 'came coded as 20713', 'incorrect stock entry.',
    "thrown away-can't sell",'Thrown away-rusty','Thrown away.','Given away','historic computer difference?....se',
    'alan hodge cant mamage this section',"thrown away-can't sell.", 'label mix up','sold in set?','mix up with c'
]

is_special_item += [
    'MIA', '?display?', 'Amazon Adjustment', 'Lighthouse Trading zero invc incorr', 
    'Dotcomgiftshop Gift Voucher £100.00', 'sold as set?', 
    'High Resolution Image', 'John Lewis','Bank Charges','Next Day Carriage'
]

is_modification_item += [
    'Adjustment', 'OOPS ! adjustment', 'reverse 21/5/10 adjustment', 'reverse previous adjustment', 
    'marked as 23343', 'incorrectly put back into stock', 'Not rcvd in 10/11/2010 delivery', 'Display', 
    'Had been put aside.',  'sold as set by dotcom', 'add stock to allocate online orders', 
    'allocate stock for dotcom orders ta', 'for online retail orders', 'Marked as 23343'
]

unclassified_items = set(description_list) - set(is_error_item) - set(is_special_item) - set(is_modification_item)


items['is_unknown_item'] = items['item_description'].str.lower().isin(is_unknown_item)
items['is_error_item'] = items['item_description'].str.lower().isin(is_error_item)
items['is_special_item'] = items['item_description'].str.lower().isin(is_special_item)
items['is_modification_item'] = items['item_description'].str.lower().isin(is_modification_item)

items = items[['item_uuid', 'item_id', 'item_family_id', 'item_description', 'item_variant', 'is_operational_item', 'is_unknown_item', 'is_special_item', 'is_modification_item', 'is_error_item']]

items.loc[items['is_operational_item'], ['item_variant', 'is_unknown_item', 'is_special_item', 'is_modification_item', 'is_error_item']] = [None, False, False, False, False]
items.loc[items['is_unknown_item']] = items.loc[items['is_unknown_item']].replace('', None)
items.loc[items['is_special_item']] = items.loc[items['is_special_item']].replace('', None)
items.loc[items['is_modification_item']] = items.loc[items['is_modification_item']].replace('', None)
items.loc[items['is_error_item']] = items.loc[items['is_error_item']].replace('', None)

dim_items = items.copy()
dim_items['item_family_id'] = dim_items['item_family_id'].astype('Int64')



