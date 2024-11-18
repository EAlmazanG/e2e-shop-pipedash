import hashlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, dayofweek, when, date_format, to_date
from pyspark.sql.types import IntegerType, DateType, StringType, BooleanType, StructType, StructField

### additional functions
def generate_hash(*cols):
    combined_string = "_".join(map(str, cols))
    return hashlib.md5(combined_string.encode()).hexdigest()

### paths
s3_bucket_name = "e2e-shop-bucket"
input_path_raw_main_product_descriptions = "raw/main_product_descriptions.csv"
input_path_retail = "raw/retail.csv"
output_clean = "clean/"

### initialize spark session
spark = SparkSession.builder.appName("pyspark-e2e-shop-session").getOrCreate()


### table transformations
def create_dim_item_family(input_path, spark, s3_bucket_name, output_path="clean"):
    # read csv source from s3
    full_input_path = f"s3://{s3_bucket_name}/{input_path}"
    raw_main_product_descriptions = spark.read.option("header", True).csv(full_input_path)

    # create dim_item_family
    dim_item_family = raw_main_product_descriptions.select(
        col("item_family").alias("item_family_id").cast(IntegerType()),
        col("item_family_description").cast(StringType()),
        col("category").alias("item_category").cast(StringType())
    ).drop_duplicates()

    full_output_path = f"s3://{s3_bucket_name}/{output_path}/dim_item_family/"
    dim_item_family.write.mode("overwrite").parquet(full_output_path)
    
    print(f"dim_item_family saved successfully to {full_output_path}")

date_start = "2009-01-01"
date_end = "2025-12-31"

def create_dim_date(date_start, date_end, spark, s3_bucket_name, output_path="clean"):
    # generate date range using Spark SQL
    date_range = spark.sql(f"""
        SELECT 
            sequence(to_date('{date_start}'), to_date('{date_end}'), interval 1 day) as dates
    """).selectExpr("explode(dates) as date")
    
    # create dim_date
    dim_date = date_range.withColumn("date_id", expr("row_number() over (order by date)")) \
        .withColumn("year", expr("year(date)").cast(IntegerType())) \
        .withColumn("month", expr("month(date)").cast(IntegerType())) \
        .withColumn("day", expr("day(date)").cast(IntegerType())) \
        .withColumn("day_of_week", date_format(col("date"), "EEEE").cast(StringType())) \
        .withColumn("is_weekend", when(dayofweek(col("date")).isin(1, 7), lit(True)).otherwise(lit(False)).cast(BooleanType()))
    
    full_output_path = f"s3://{s3_bucket_name}/{output_path}/dim_date/"
    dim_date.write.mode("overwrite").parquet(full_output_path)
    
    print(f"dim_date saved successfully to {full_output_path}")

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

def create_dim_location(country_data, spark, s3_bucket_name, output_path = "clean"):
    # country dict to list of tuples
    country_data_tuples = list(zip(
        country_data['country_id'], 
        country_data['iso_country_code'], 
        country_data['country_name'], 
        country_data['country_code_name']
    ))

    # schema
    schema = StructType([
        StructField("country_id", IntegerType(), True),
        StructField("iso_country_code", StringType(), True),
        StructField("country_name", StringType(), True),
        StructField("country_code_name", StringType(), True)
    ])

    # create_dim_location
    dim_location = spark.createDataFrame(country_data_tuples, schema=schema)

    full_output_path = f"s3://{s3_bucket_name}/{output_path}/dim_location/"
    dim_location.write.mode("overwrite").parquet(full_output_path)

    print(f"dim_location saved successfully to {full_output_path}")

def create_fact_transactions(input_path, dim_date_path, dim_location_path, spark, s3_bucket_name, output_path="clean"):
    # load raw_retail data
    full_input_path = f"s3://{s3_bucket_name}/{input_path}"
    raw_retail = spark.read.csv(full_input_path, header=True, inferSchema=True)
    
    # rename columns to match schema
    transactions = raw_retail.selectExpr(
        "InvoiceNo as invoice_id",
        "StockCode as item_id",
        "Description as item_description",
        "cast(Quantity as int) as quantity_amount",
        "to_timestamp(InvoiceDate, 'yyyy-MM-dd HH:mm:ss') as event_timestamp_invoiced_at",
        "cast(UnitPrice as float) as unit_price_eur",
        "cast(CustomerID as int) as customer_id",
        "Country as country_name"
    ).withColumn("transaction_id", expr("monotonically_increasing_id()")) \
     .withColumn("item_uuid", expr("sha2(concat_ws('_', item_id, item_description), 256)"))
    
    # load dim_date and dim_location for mapping
    full_dim_date_path = f"s3://{s3_bucket_name}/{dim_date_path}"
    full_dim_location_path = f"s3://{s3_bucket_name}/{dim_location_path}"

    dim_date = spark.read.parquet(full_dim_date_path)
    dim_location = spark.read.parquet(full_dim_location_path)

    # join transactions with dim_date
    fact_transactions = transactions.withColumn("transaction_date_id", to_date(col("event_timestamp_invoiced_at"))) \
        .join(dim_date.select("date_id", "date"), col("transaction_date_id") == col("date"), "left") \
        .withColumnRenamed("date_id", "transaction_date_id") \
        .drop("date")
    
    # join transactions with dim_location
    fact_transactions = fact_transactions.join(
        dim_location.select("country_id", "country_name"),
        "country_name",
        "left"
    )
    
    # calculate total_price_eur and reorder columns
    fact_transactions = fact_transactions.withColumn(
        "total_price_eur",
        col("quantity_amount") * col("unit_price_eur")
    ).select(
        "transaction_id",
        "invoice_id",
        "event_timestamp_invoiced_at",
        "transaction_date_id",
        "item_uuid",
        "item_id",
        "quantity_amount",
        "unit_price_eur",
        "total_price_eur",
        "customer_id",
        "country_id"
    )

    full_output_path = f"s3://{s3_bucket_name}/{output_path}/fact_transactions/"
    fact_transactions.write.mode("overwrite").parquet(full_output_path)
    
    print(f"fact_transactions saved successfully to {full_output_path}")