import hashlib
from faker import Faker
import random
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, expr, dayofweek, when, date_format, to_date, lower, length, row_number, collect_list, array_contains, round, to_timestamp, rand
from pyspark.sql.types import IntegerType, DateType, StringType, BooleanType, StructType, StructField
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

def generate_hash(*cols):
    combined_string = "_".join(map(str, cols))
    return hashlib.md5(combined_string.encode()).hexdigest()

def generate_names(country, countries_locale):
    locale = countries_locale.get(country, 'en_US')
    faker = Faker(locale)
    first_names = [faker.first_name() for _ in range(30)]
    last_names = [faker.last_name() for _ in range(30)]
    full_names = [f"{random.choice(first_names)} {random.choice(last_names)}" for _ in range(30)]
    return full_names

def assign_name(country, names_by_country):
    if country in names_by_country:
        return random.choice(names_by_country[country])
    return "Unknown"

def initialize_spark(app_name="pyspark-e2e-shop-session"):
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")  # Optional
    return spark

def load_data_from_s3(spark, s3_bucket, input_path):
    full_path = f"s3://{s3_bucket}/{input_path}"
    return spark.read.option("header", True).csv(full_path)

def save_data_to_s3(df, s3_bucket, output_path):
    full_path = f"s3://{s3_bucket}/{output_path}"
    df.write.mode("overwrite").parquet(full_path)
    print(f"Data saved successfully to {full_path}")

def create_dim_item_family(spark, s3_bucket, input_path, output_path):
    raw_data = load_data_from_s3(spark, s3_bucket, input_path)
    dim_item_family = raw_data.select(
        col("item_family").alias("item_family_id").cast(IntegerType()),
        col("item_family_description").cast(StringType()),
        col("category").alias("item_category").cast(StringType())
    ).drop_duplicates()
    save_data_to_s3(dim_item_family, s3_bucket, output_path)

def create_dim_date(spark, s3_bucket, date_start, date_end, output_path):
    date_range = spark.sql(f"""
        SELECT sequence(to_date('{date_start}'), to_date('{date_end}'), interval 1 day) as dates
    """).selectExpr("explode(dates) as date")

    dim_date = date_range.withColumn("date_id", expr("row_number() over (order by date)")) \
        .withColumn("year", expr("year(date)").cast(IntegerType())) \
        .withColumn("month", expr("month(date)").cast(IntegerType())) \
        .withColumn("day", expr("day(date)").cast(IntegerType())) \
        .withColumn("day_of_week", date_format(col("date"), "EEEE").cast(StringType())) \
        .withColumn("is_weekend", when(dayofweek(col("date")).isin(1, 7), lit(True)).otherwise(lit(False)).cast(BooleanType()))
    
    save_data_to_s3(dim_date, s3_bucket, output_path)

def create_dim_location(spark, s3_bucket, country_data, output_path):
    schema = StructType([
        StructField("country_id", IntegerType(), True),
        StructField("iso_country_code", StringType(), True),
        StructField("country_name", StringType(), True),
        StructField("country_code_name", StringType(), True)
    ])
    dim_location = spark.createDataFrame(data=list(zip(
        country_data["country_id"],
        country_data["iso_country_code"],
        country_data["country_name"],
        country_data["country_code_name"]
    )), schema=schema)
    save_data_to_s3(dim_location, s3_bucket, output_path)

def create_stg_transactions(spark, s3_bucket, input_path, output_path):
    raw_data = load_data_from_s3(spark, s3_bucket, input_path)

    stg_transactions = raw_data.selectExpr(
        "InvoiceNo as invoice_id",
        "StockCode as item_id",
        "Description as item_description",
        "cast(Quantity as int) as quantity_amount",
        "cast(UnitPrice as float) as unit_price_eur",
        "cast(CustomerID as int) as customer_id",
        "Country as country_name",
        "InvoiceDate as raw_invoice_date"
    ).withColumn(
        "event_timestamp_invoiced_at",
        to_timestamp(col("raw_invoice_date"), "M/d/yy H:mm")
    ).withColumn(
        "transaction_id", expr("monotonically_increasing_id()")
    ).withColumn(
        "item_uuid", expr("sha2(concat_ws('_', item_id, item_description), 256)")
    ).withColumn(
        "date", to_date(col("event_timestamp_invoiced_at"))
    )
    
    save_data_to_s3(stg_transactions, s3_bucket, output_path)

def create_fact_transactions(spark, s3_bucket, input_path, dim_date_path, dim_location_path, output_path):
    stg_transactions = spark.read.parquet(f"s3://{s3_bucket}/{input_path}")
    dim_date = spark.read.parquet(f"s3://{s3_bucket}/{dim_date_path}")
    dim_location = spark.read.parquet(f"s3://{s3_bucket}/{dim_location_path}")

    fact_transactions = stg_transactions.join(
        dim_date,
        stg_transactions["date"] == dim_date["date"],
        "left"
    )

    fact_transactions = fact_transactions.join(
        dim_location.select("country_name", "country_id"),
        "country_name",
        "left"
    )

    fact_transactions = fact_transactions.withColumn(
        "total_price_eur",
        round(col("quantity_amount") * col("unit_price_eur"), 2)
    )

    fact_transactions = fact_transactions.select(
        "transaction_id",
        "invoice_id",
        "event_timestamp_invoiced_at",
        "date_id",
        "item_uuid",
        "item_id",
        "quantity_amount",
        "unit_price_eur",
        "total_price_eur",
        "customer_id",
        "country_id"
    )

    save_data_to_s3(fact_transactions, s3_bucket, output_path)
    
def create_dim_items(spark, s3_bucket, input_path, output_path):
    stg_transactions = spark.read.parquet(f"s3://{s3_bucket}/{input_path}")

    items = stg_transactions.groupBy("item_uuid", "item_id", "item_description").count()

    items = items.withColumn(
        "is_operational_item",
        (length(col("item_id")) < 5) | col("item_id").rlike("(?i)gift")
    ).withColumn(
        "item_family_id",
        when(col("item_id").rlike("^[0-9]+"), expr("regexp_extract(item_id, '^([0-9]+)', 1)")).otherwise(None)
    ).withColumn(
        "item_variant",
        when(col("item_id").rlike("[A-Za-z]+$"), expr("regexp_extract(item_id, '([A-Za-z]+)$', 1)")).otherwise("")
    ).withColumn(
        "item_variant",
        when(col("item_id") == col("item_family_id"), lit(None)).otherwise(col("item_variant"))
    )

    is_unknown_item = ['?','??','???','Incorrect stock entry.',"can't find",'nan',None]
    is_error_item = [
        'check',
        'damaged',
        'wet/rusty',
        'found',
        'lost in space',
        'wrongly marked. 23343 in box',
        'wrongly marked 23343',
        'wrongly coded 23343',
        'wrongly coded-23343',
        'Found',
        'damages',
        'damages/display',
        'WET/MOULDY',
        'damages?',
        'Damaged',
        'wet',
        'wet rusty',
        'wrongly marked',
        'broken',
        'CHECK',
        'wrong barcode',
        '?missing',
        'smashed',
        'missing',
        '???lost',
        'faulty',
        'Missing',
        '?lost',
        'wrongly sold (22719) barcode',
        'wrong code?',
        'wet boxes',
        '?? missing',
        'missing?',
        'lost',
        'mouldy, thrown away.',
        'mouldy, unsaleable.',
        'damages/showroom etc',
        'wrong barcode (22467)',
        'wrong code',
        'FOUND',
        'Wrongly mrked had 85123a in box',
        'water damage',
        'rusty throw away',
        'Crushed',
        'mouldy',
        '20713 wrongly marked',
        'wrongly coded 20713',
        'sold with wrong barcode',
        'Sale error',
        'Found by jackie',
        'found some more on shelf',
        'damages/dotcom?',
        'damaged stock',
        'found box',
        'throw away',
        'Wet pallet-thrown away',
        'wet pallet',
        '???missing',
        '????missing',
        'Water damaged',
        'damages/credits from ASOS.',
        'Unsaleable, destroyed.',
        'crushed',
        'crushed ctn',
        'cracked',
        'Damages/samples',
        'water damaged',
        'wet damaged',
        'wet?',
        'damages wax',
        'stock creditted wrongly',
        '????damages????',
        'check?',
        'wrongly marked carton 22804',
        'Damages',
        'samples/damages',
        'DAMAGED',
        'wrongly sold as sets',
        'wrongly sold sets',
        'Found in w/hse',
        'lost??',
        'stock check',
        'crushed boxes',
        'on cargo order',
        'test',
        'barcode problem',
        'mixed up',
        'michel oops',
        'printing smudges/thrown away',
        'had been put aside',
        'rusty thrown away',
        'incorrectly made-thrown away.',
        'Breakages',
        'counted',
        'Had been put aside.',
        'returned',
        'thrown away',
        'mystery! Only ever imported 1800',
        'Dagamed',
        'code mix up? 84930',
        'Printing smudges/thrown away',
        'came coded as 20713',
        'incorrect stock entry.',
        "thrown away-can't sell",
        'Thrown away-rusty',
        'Thrown away.',
        'Given away',
        'historic computer difference?....se',
        'alan hodge cant mamage this section',
        "thrown away-can't sell.",
        'label mix up',
        'sold in set?',
        'mix up with c'
    ]
    is_special_item = [
        'dotcom',
        'Amazon Adjustment',
        'sold as set on dotcom',
        'Sold as 1 on dotcom',
        'rcvd be air temp fix for dotcom sit',
        're dotcom quick fix.',
        'sold as set on dotcom and amazon',
        'showroom',
        'amazon adjust',
        'Amazon',
        'John Lewis',
        'Dotcomgiftshop Gift Voucher £100.00',
        "Dotcom sold in 6's",
        'amazon',
        'amazon sales',
        'AMAZON',
        'CORDIAL JUG',
        'allocate stock for dotcom orders ta',
        'website fixed',
        'dotcom adjust',
        'sold as set/6 by dotcom',
        'sold as set by dotcom',
        'Dotcom sales',
        'dotcom sales',
        'Dotcom',
        'dotcomstock',
        'FBA',
        'Dotcom set',
        'dotcom sold sets',
        'Amazon sold sets',
        'ebay',
        'MIA',
        '?display?',
        'Amazon Adjustment',
        'Lighthouse Trading zero invc incorr',
        'Dotcomgiftshop Gift Voucher £100.00',
        'sold as set?',
        'High Resolution Image',
        'John Lewis',
        'Bank Charges',
        'Next Day Carriage'
    ]
    is_modification_item = [
        'Adjustment',
        'adjustment',
        'taig adjust no stock',
        'Show Samples',
        'samples',
        'sold as 1',
        'OOPS ! adjustment',
        'taig adjust',
        'reverse 21/5/10 adjustment',
        'sold as 22467',
        'add stock to allocate online orders',
        'temp adjustment',
        'mailout ',
        'mailout',
        're-adjustment',
        'did  a credit  and did not tick ret',
        'incorrectly credited C550456 see 47',
        'reverse previous adjustment',
        'adjust',
        'label mix up',
        '?sold as sets?',
        '? sold as sets?',
        'Adjustment',
        'OOPS ! adjustment',
        'reverse 21/5/10 adjustment',
        'reverse previous adjustment',
        'marked as 23343',
        'incorrectly put back into stock',
        'Not rcvd in 10/11/2010 delivery',
        'Display',
        'Had been put aside.',
        'sold as set by dotcom',
        'add stock to allocate online orders',
        'allocate stock for dotcom orders ta',
        'for online retail orders',
        'Marked as 23343'
    ]

    items = items.withColumn("is_unknown_item", col("item_description").isin(is_unknown_item).cast("boolean"))
    items = items.withColumn("is_error_item", col("item_description").isin(is_error_item).cast("boolean"))
    items = items.withColumn("is_special_item", col("item_description").isin(is_special_item).cast("boolean"))
    items = items.withColumn("is_modification_item", col("item_description").isin(is_modification_item).cast("boolean"))

    items = items.fillna({
        "is_operational_item": False,
        "is_unknown_item": False,
        "is_special_item": False,
        "is_modification_item": False,
        "is_error_item": False
    })

    items = items.withColumn("item_family_id", col("item_family_id").cast("int"))

    items = items.select(
        "item_uuid",
        "item_id",
        "item_family_id",
        "item_description",
        "item_variant",
        "is_operational_item",
        "is_unknown_item",
        "is_special_item",
        "is_modification_item",
        "is_error_item"
    )

    save_data_to_s3(items, s3_bucket, output_path)

def create_dim_customers(spark, s3_bucket, input_path, output_path):
    stg_transactions = spark.read.parquet(f"s3://{s3_bucket}/{input_path}")

    customers = stg_transactions.select("customer_id", "country_name") \
        .filter(col("customer_id").isNotNull()) \
        .groupBy("customer_id") \
        .agg(expr("first(country_name) as country_name"))

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

    names_data = [(country, name) for country, locale in countries_locale.items()
                  for name in generate_names(country, countries_locale)]
    names_df = spark.createDataFrame(names_data, ["country_name", "customer_name"])

    customers = customers.join(names_df, on="country_name", how="left")
    customers = customers.withColumn("random_value", rand())
    window_spec = Window.partitionBy("customer_id").orderBy("random_value")
    customers = customers.withColumn("row_number", row_number().over(window_spec))
    customers = customers.filter(col("row_number") == 1).drop("row_number", "random_value")
    customers = customers.withColumn(
        "customer_name",
        when(col("customer_name").isNull(), lit("Unknown")).otherwise(col("customer_name"))
    )

    dim_customers = customers.select("customer_id", "customer_name").drop_duplicates()
    save_data_to_s3(dim_customers, s3_bucket, output_path)

def main():
    s3_bucket = "e2e-shop-bucket"
    input_path_raw_main_product_descriptions = "raw/main_product_descriptions.csv"
    input_path_retail = "raw/retail.csv"

    date_start = "2009-01-01"
    date_end = "2025-12-31"

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

    spark = initialize_spark()
    
    # Create stg tables
    create_stg_transactions(spark, s3_bucket, input_path_retail, "clean/stg_transactions/")

    # Create init dimensions
    create_dim_item_family(spark, s3_bucket, input_path_raw_main_product_descriptions, "clean/dim_item_family/")
    create_dim_date(spark, s3_bucket, date_start, date_end, "clean/dim_date/")
    create_dim_location(spark, s3_bucket, country_data, "clean/dim_location/")

    # Create complex dimensions
    create_dim_items(spark, s3_bucket, "clean/stg_transactions/", "clean/dim_items/")
    create_dim_customers(spark, s3_bucket, "clean/stg_transactions/", "clean/dim_customers/")
    
    # Create fact table
    create_fact_transactions(spark, s3_bucket, "clean/stg_transactions/", "clean/dim_date/", "clean/dim_location/", "clean/fact_transactions/")
    
if __name__ == "__main__":
    main()