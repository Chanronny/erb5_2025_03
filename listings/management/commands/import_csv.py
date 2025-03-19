import os
import csv
import logging
from datetime import datetime
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from listings.models import Listing
from realtors.models import Realtor
from listings.choices import district_choices

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_import.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Import data from CSV files into the database'

    def add_arguments(self, parser):
        parser.add_argument('--model', type=str, required=True, choices=['realtor', 'listing'],
                            help='Model to import data for (realtor or listing)')
        parser.add_argument('--file', type=str, required=True,
                            help='Path to the CSV file')

    def handle(self, *args, **options):
        model = options['model']
        file_path = options['file']
        
        if not os.path.exists(file_path):
            raise CommandError(f'File {file_path} does not exist')
        
        self.stdout.write(self.style.SUCCESS(f'Starting import for model: {model} from file: {file_path}'))
        
        try:
            if model == 'realtor':
                count = self.import_realtors(file_path)
            else:
                count = self.import_listings(file_path)
            
            self.stdout.write(self.style.SUCCESS(f'Successfully imported {count} {model}s'))
        
        except Exception as e:
            logger.error(f'Error during import: {e}')
            raise CommandError(f'Import failed: {e}')

    def import_realtors(self, file_path):
        """Import realtors from CSV file."""
        realtors_created = 0
        
        try:
            with open(file_path, 'r', encoding='utf-8') as csv_file:
                reader = csv.DictReader(csv_file)
                
                for i, row in enumerate(reader, start=1):
                    try:
                        with transaction.atomic():
                            # Validate required fields
                            if not row.get('name') or not row.get('email') or not row.get('phone'):
                                logger.warning(f'Row {i}: Missing required fields (name, email, or phone)')
                                continue
                            
                            # Process hire_date
                            hire_date = None
                            if row.get('hire_date'):
                                try:
                                    hire_date = datetime.strptime(row['hire_date'], '%Y-%m-%d')
                                except ValueError:
                                    logger.warning(f'Row {i}: Invalid date format for hire_date')
                            
                            # Create realtor
                            realtor = Realtor(
                                name=row['name'],
                                photo=row.get('photo', ''),
                                description=row.get('description', ''),
                                phone=row['phone'],
                                email=row['email'],
                                is_mvp=row.get('is_mvp', '').lower() == 'true',
                            )
                            
                            # Only set hire_date if it was provided and valid
                            if hire_date:
                                realtor.hire_date = hire_date
                            
                            realtor.save()
                            realtors_created += 1
                            logger.info(f'Created realtor: {realtor.name}')
                    
                    except Exception as e:
                        logger.error(f'Error processing row {i}: {e}')
        
        except Exception as e:
            logger.error(f'Error reading CSV file: {e}')
            raise
        
        return realtors_created

    def import_listings(self, file_path):
        """Import listings from CSV file."""
        listings_created = 0
        valid_districts = list(district_choices.keys())
        
        try:
            with open(file_path, 'r', encoding='utf-8') as csv_file:
                reader = csv.DictReader(csv_file)
                
                for i, row in enumerate(reader, start=1):
                    try:
                        with transaction.atomic():
                            # Validate required fields
                            if not row.get('title') or not row.get('price') or not row.get('realtor_id'):
                                logger.warning(f'Row {i}: Missing required fields (title, price, or realtor_id)')
                                continue
                            
                            # Validate realtor exists
                            try:
                                realtor_id = int(row['realtor_id'])
                                realtor = Realtor.objects.get(id=realtor_id)
                            except (ValueError, Realtor.DoesNotExist):
                                logger.warning(f'Row {i}: Invalid realtor_id: {row.get("realtor_id")}')
                                continue
                            
                            # Validate district
                            district = row.get('district')
                            if district and district not in valid_districts:
                                logger.warning(f'Row {i}: Invalid district: {district}')
                                continue
                            
                            # Process list_date
                            list_date = None
                            if row.get('list_date'):
                                try:
                                    list_date = datetime.strptime(row['list_date'], '%Y-%m-%d')
                                except ValueError:
                                    logger.warning(f'Row {i}: Invalid date format for list_date')
                            
                            # Create listing
                            listing = Listing(
                                realtor=realtor,
                                title=row['title'],
                                address=row.get('address', ''),
                                street=row.get('street', ''),
                                district=row.get('district', ''),
                                description=row.get('description', ''),
                                price=int(row['price']),
                                bedrooms=int(row.get('bedrooms', 0)),
                                bathrooms=float(row.get('bathrooms', 0)),
                                clubhouse=int(row.get('clubhouse', 0)),
                                sqft=int(row.get('sqft', 0)),
                                estate_size=float(row.get('estate_size', 0)),
                                is_published=row.get('is_published', '').lower() == 'true',
                                photo_main=row.get('photo_main', ''),
                                photo_1=row.get('photo_1', ''),
                                photo_2=row.get('photo_2', ''),
                                photo_3=row.get('photo_3', ''),
                                photo_4=row.get('photo_4', ''),
                                photo_5=row.get('photo_5', ''),
                                photo_6=row.get('photo_6', '')
                            )
                            
                            # Only set list_date if it was provided and valid
                            if list_date:
                                listing.list_date = list_date
                            
                            listing.save()
                            listings_created += 1
                            logger.info(f'Created listing: {listing.title}')
                    
                    except Exception as e:
                        logger.error(f'Error processing row {i}: {e}')
        
        except Exception as e:
            logger.error(f'Error reading CSV file: {e}')
            raise
        
        return listings_created
