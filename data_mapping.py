import pandas as pd
from sqlalchemy import create_engine
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataDictionaryMapper:
    def __init__(self, db_config):
        """Initialize database connection and configuration"""
        self.engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        self.mapping_rules = {}
        
    def load_data_dictionaries(self, platform_dict_path, client_dict_path):
        """Load platform and client data dictionaries"""
        try:
            self.platform_dict = pd.read_excel(platform_dict_path)
            self.client_dict = pd.read_excel(client_dict_path)
            logger.info(f"Successfully loaded platform dictionary: {len(self.platform_dict)} records")
            logger.info(f"Successfully loaded client dictionary: {len(self.client_dict)} records")
        except Exception as e:
            logger.error(f"Failed to load data dictionaries: {str(e)}")
            raise
    
    def set_mapping_rules(self, rules):
        """Set mapping rules"""
        self.mapping_rules = rules
        logger.info(f"Mapping rules set: {rules}")
        
    def perform_mapping(self):
        """Perform code mapping"""
        if not hasattr(self, 'platform_dict') or not hasattr(self, 'client_dict'):
            raise ValueError("Please load data dictionaries first")
            
        # Create result DataFrame
        self.mapping_result = pd.DataFrame(columns=[
            'platform_code', 'platform_name', 
            'client_code', 'client_name',
            'match_score', 'match_status'
        ])
        
        # Mapping logic implementation (example: based on name similarity)
        for _, platform_row in self.platform_dict.iterrows():
            best_match = {
                'client_code': None,
                'client_name': None,
                'match_score': 0,
                'match_status': 'Unmatched'
            }
            
            # Simple example: match based on name similarity
            for _, client_row in self.client_dict.iterrows():
                score = self._calculate_similarity(
                    platform_row[self.mapping_rules['platform_name_field']],
                    client_row[self.mapping_rules['client_name_field']]
                )
                
                if score > best_match['match_score']:
                    best_match = {
                        'client_code': client_row[self.mapping_rules['client_code_field']],
                        'client_name': client_row[self.mapping_rules['client_name_field']],
                        'match_score': score,
                        'match_status': 'Auto-matched' if score >= self.mapping_rules['threshold'] else 'Possible match'
                    }
            
            # Add to results
            self.mapping_result = pd.concat([self.mapping_result, pd.DataFrame({
                'platform_code': [platform_row[self.mapping_rules['platform_code_field']]],
                'platform_name': [platform_row[self.mapping_rules['platform_name_field']]],
                'client_code': [best_match['client_code']],
                'client_name': [best_match['client_name']],
                'match_score': [best_match['match_score']],
                'match_status': [best_match['match_status']]
            })], ignore_index=True)
            
        logger.info(f"Mapping completed: {len(self.mapping_result)} records in total, "
                   f"{len(self.mapping_result[self.mapping_result['match_status'] == 'Auto-matched'])} auto-matched")
        return self.mapping_result
        
    def _calculate_similarity(self, str1, str2):
        """Calculate similarity between two strings (simplified example)"""
        # In practice, more sophisticated algorithms like edit distance or cosine similarity can be used
        if not str1 or not str2:
            return 0
            
        str1 = str(str1).lower().strip()
        str2 = str(str2).lower().strip()
        
        # Simple character match ratio calculation
        common_chars = set(str1) & set(str2)
        return len(common_chars) / max(len(set(str1)), len(set(str2)))
    
    def save_to_database(self, table_name='data_mapping_result'):
        """Save mapping results to database"""
        if not hasattr(self, 'mapping_result'):
            raise ValueError("Please perform mapping first")
            
        try:
            self.mapping_result.to_sql(
                table_name, 
                self.engine, 
                if_exists='replace', 
                index=False
            )
            logger.info(f"Mapping results saved to database table: {table_name}")
        except Exception as e:
            logger.error(f"Failed to save mapping results to database: {str(e)}")
            raise

# Example usage
if __name__ == "__main__":
    # Database configuration
    db_config = {
        'user': 'gnuhealth',
        'password': 'your_password',
        'host': 'localhost',
        'port': 5432,
        'database': 'gnuhealthdb'
    }
    
    # Create mapper instance
    mapper = DataDictionaryMapper(db_config)
    
    # Load data dictionaries
    mapper.load_data_dictionaries(
        platform_dict_path='platform_data_dictionary.xlsx',
        client_dict_path='client_data_dictionary.xlsx'
    )
    
    # Set mapping rules
    mapper.set_mapping_rules({
        'platform_code_field': 'code',
        'platform_name_field': 'name',
        'client_code_field': 'code',
        'client_name_field': 'name',
        'threshold': 0.7  # Similarity threshold
    })
    
    # Perform mapping
    result = mapper.perform_mapping()
    
    # Save results to database
    mapper.save_to_database()  