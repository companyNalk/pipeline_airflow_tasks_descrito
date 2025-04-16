"""
Agendor CRM module for data extraction.
This module handles the extraction of data from Agendor CRM API,
including deals, organizations, and people.
"""

import os
import subprocess
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

def run_deals(customer):
    """
    Run the deals.py script to extract deals data from Agendor CRM.
    
    Args:
        customer (dict): Customer information dictionary
    
    Returns:
        str: Result message from the script execution
    """
    logger.info(f"Extracting deals data for customer {customer['id']}")
    try:
        # Set environment variables for the script
        env = os.environ.copy()
        env["API_BASE_URL"] = customer.get("api_base_url", "")
        env["API_TOKEN"] = customer.get("api_token", "")
        env["BUCKET_NAME"] = customer.get("bucket_name", "")
        env["GOOGLE_APPLICATION_CREDENTIALS"] = customer.get("credentials_path", "/opt/airflow/config/gcp.json")
        
        # Run the deals.py script
        result = subprocess.run(["python", "deals.py"], env=env, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"Error extracting deals: {result.stderr}")
            return f"Error extracting deals: {result.stderr}"
        
        logger.info(f"Deals extraction completed: {result.stdout}")
        return result.stdout
    except Exception as e:
        logger.error(f"Exception during deals extraction: {str(e)}")
        return f"Exception during deals extraction: {str(e)}"

def run_organizations(customer):
    """
    Run the organizations.py script to extract organizations data from Agendor CRM.
    
    Args:
        customer (dict): Customer information dictionary
    
    Returns:
        str: Result message from the script execution
    """
    logger.info(f"Extracting organizations data for customer {customer['id']}")
    try:
        # Set environment variables for the script
        env = os.environ.copy()
        env["API_BASE_URL"] = customer.get("api_base_url", "")
        env["API_TOKEN"] = customer.get("api_token", "")
        env["BUCKET_NAME"] = customer.get("bucket_name", "")
        env["GOOGLE_APPLICATION_CREDENTIALS"] = customer.get("credentials_path", "")
        
        # Run the organizations.py script
        result = subprocess.run(["python", "organizations.py"], env=env, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"Error extracting organizations: {result.stderr}")
            return f"Error extracting organizations: {result.stderr}"
        
        logger.info(f"Organizations extraction completed: {result.stdout}")
        return result.stdout
    except Exception as e:
        logger.error(f"Exception during organizations extraction: {str(e)}")
        return f"Exception during organizations extraction: {str(e)}"

def run_people(customer):
    """
    Run the people.py script to extract people data from Agendor CRM.
    
    Args:
        customer (dict): Customer information dictionary
    
    Returns:
        str: Result message from the script execution
    """
    logger.info(f"Extracting people data for customer {customer['id']}")
    try:
        # Set environment variables for the script
        env = os.environ.copy()
        env["API_BASE_URL"] = customer.get("api_base_url", "")
        env["API_TOKEN"] = customer.get("api_token", "")
        env["BUCKET_NAME"] = customer.get("bucket_name", "")
        env["GOOGLE_APPLICATION_CREDENTIALS"] = customer.get("credentials_path", "")
        
        # Run the people.py script
        result = subprocess.run(["python", "people.py"], env=env, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"Error extracting people: {result.stderr}")
            return f"Error extracting people: {result.stderr}"
        
        logger.info(f"People extraction completed: {result.stdout}")
        return result.stdout
    except Exception as e:
        logger.error(f"Exception during people extraction: {str(e)}")
        return f"Exception during people extraction: {str(e)}"

def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Agendor CRM.
    
    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'extract_organizations',
            'python_callable': run_organizations
        },
        {
            'task_id': 'extract_people',
            'python_callable': run_people
        },
        {
            'task_id': 'extract_deals',
            'python_callable': run_deals
        }
    ]
