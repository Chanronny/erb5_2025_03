# CSV to PostgreSQL Data Import Tool

This tool allows you to import data from CSV files into the PostgreSQL database for the BCRE Django project. It supports importing data for both Realtors and Listings models.

## Requirements

- Python 3.x
- psycopg2 (PostgreSQL adapter for Python)
- Access to the PostgreSQL database

You can install the required packages using pip:

```bash
pip install psycopg2-binary
```

## Usage

The script takes two required arguments:
- `--model`: The model to import data for (either 'realtor' or 'listing')
- `--file`: The path to the CSV file containing the data

### Examples

Import realtor data:
```bash
python upload_data_test.py --model realtor --file sample_realtors.csv
```

Import listing data:
```bash
python upload_data_test.py --model listing --file sample_listings.csv
```

## CSV File Format

### Realtor CSV Format

The CSV file for realtors should have the following columns:

| Column | Description | Required | Format |
|--------|-------------|----------|--------|
| name | Realtor's name | Yes | String |
| photo | Path to photo file | No | String (path) |
| description | Realtor's description | No | String |
| phone | Phone number | Yes | String |
| email | Email address | Yes | String |
| is_mvp | Whether the realtor is an MVP | No | 'true' or 'false' |
| hire_date | Date the realtor was hired | No | YYYY-MM-DD |

### Listing CSV Format

The CSV file for listings should have the following columns:

| Column | Description | Required | Format |
|--------|-------------|----------|--------|
| realtor_id | ID of the realtor | Yes | Integer |
| title | Listing title | Yes | String |
| address | Full address | No | String |
| street | Street name | No | String |
| district | District name | No | Must be one of the valid districts |
| description | Property description | No | String |
| price | Price in HKD | Yes | Integer |
| bedrooms | Number of bedrooms | No | Integer |
| bathrooms | Number of bathrooms | No | Decimal (e.g., 2.5) |
| clubhouse | Clubhouse availability | No | Integer (0 or 1) |
| sqft | Square footage | No | Integer |
| estate_size | Estate size | No | Float |
| is_published | Whether the listing is published | No | 'true' or 'false' |
| list_date | Date the property was listed | No | YYYY-MM-DD |
| photo_main | Main photo path | No | String (path) |
| photo_1 to photo_6 | Additional photo paths | No | String (path) |

## Valid Districts

The following districts are valid for the 'district' field in listings:

- Islands
- Kwai Tsing
- Sai Kung
- Tsuen Wan
- Tuen Mun
- Yuen Long
- Wong Tai Sin
- Sha Tin
- Tai Po
- Kowloon City
- Kwun Tong
- Sham Shui Po
- Yau Tsim Mong
- Central & Western
- Eastern
- Southern
- Wan Chai
- North

## Data Validation

The script performs the following validations:

### For Realtors:
- Name is required
- Email is required
- Phone is required

### For Listings:
- Title is required
- Price is required
- Realtor ID is required
- District must be one of the valid districts

## Logging

The script logs information, warnings, and errors to both the console and a file named `data_import.log`. This helps track the import process and diagnose any issues.

## Foreign Key Validation

For listings, the script verifies that the specified realtor_id exists in the database before attempting to insert the listing. If a realtor_id is invalid, the listing will be skipped.

## Sample Files

Two sample CSV files are provided:
- `sample_realtors.csv`: Example of a CSV file for importing realtors
- `sample_listings.csv`: Example of a CSV file for importing listings

These files can be used as templates for creating your own data import files.
