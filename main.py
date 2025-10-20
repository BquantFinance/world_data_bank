"""
World Bank Data360 Explorer - Complete Single-File Streamlit App
Dark mode, elegant, minimalist interface for exploring global data
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import List, Dict, Any, Optional, Tuple
import json
from datetime import datetime
import time
from collections import defaultdict
import requests


# ============================================================================
# DATA360 CLIENT
# ============================================================================

class Data360Client:
    """Complete Data360 API Client"""
    BASE_URL = "https://data360api.worldbank.org"
    
    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.session = requests.Session()
        
    def search(self, query: str, top: int = 100, skip: int = 0, 
               filter_by: Optional[str] = None, orderby: Optional[str] = None) -> Dict:
        """Search Data360 - avoids wildcard searches"""
        url = f"{self.BASE_URL}/data360/searchv2"
        
        # Replace wildcard or empty search with a common term
        if not query or query.strip() == "" or query == "*":
            query = "GDP"
        
        payload = {
            "count": True,
            "select": "series_description/idno, series_description/name, series_description/database_id, series_description/description, series_description/topics, series_description/source",
            "search": query,
            "top": top,
            "skip": skip
        }
        
        if filter_by:
            payload["filter"] = filter_by
        if orderby:
            payload["orderby"] = orderby
            
        try:
            response = self.session.post(url, json=payload, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            # Return empty result on error but log it
            print(f"Search API Error: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response body: {e.response.text}")
            return {"value": [], "@odata.count": 0}
    
    def list_indicators(self, database_id: str) -> List[str]:
        """List all indicators in a database"""
        url = f"{self.BASE_URL}/data360/indicators"
        params = {"datasetId": database_id}
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()
    
    def get_indicator_metadata(self, indicator_id: str) -> Dict:
        """Get detailed metadata for an indicator"""
        url = f"{self.BASE_URL}/data360/metadata"
        query = f"&$filter=series_description/idno eq '{indicator_id}'"
        payload = {"query": query}
        response = self.session.post(url, json=payload, timeout=self.timeout)
        response.raise_for_status()
        return response.json()
    
    def get_data(self, database_id: str, indicator: Optional[str] = None, 
                 ref_area: Optional[str] = None, time_period_from: Optional[str] = None,
                 time_period_to: Optional[str] = None, auto_paginate: bool = True,
                 max_records: int = 10000) -> Dict:
        """Fetch data with optional auto-pagination"""
        url = f"{self.BASE_URL}/data360/data"
        params = {"DATABASE_ID": database_id}
        
        if indicator:
            params["INDICATOR"] = indicator
        if ref_area:
            params["REF_AREA"] = ref_area
        if time_period_from:
            params["timePeriodFrom"] = time_period_from
        if time_period_to:
            params["timePeriodTo"] = time_period_to
            
        if not auto_paginate:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        
        all_data = []
        skip = 0
        total_count = None
        
        while len(all_data) < max_records:
            params["skip"] = skip
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            result = response.json()
            
            if total_count is None:
                total_count = result.get("count", 0)
            
            batch_data = result.get("value", [])
            if not batch_data:
                break
                
            all_data.extend(batch_data)
            
            if len(all_data) >= total_count or len(all_data) >= max_records:
                break
                
            skip += len(batch_data)
            time.sleep(0.05)
        
        return {
            "count": len(all_data),
            "total_count": total_count,
            "value": all_data[:max_records]
        }


# ============================================================================
# DATABASE CATALOG & CLASSIFICATION
# Complete catalog with all 87 databases - Auto-generated
# Total Indicators: 13,607
# ============================================================================

DATABASE_CATALOG = {
    "BS_BTI": {
        "name": "Bertelsmann Transformation Index",
        "organization": "Bertelsmann Stiftung",
        "themes": ["Governance", "Democracy", "Development"],
        "description": "Political and economic transformation in developing countries",
        "indicator_count": 76
    },
    "BS_SGI": {
        "name": "Sustainable Governance Indicators",
        "organization": "Bertelsmann Stiftung",
        "themes": ["Governance", "Sustainability", "Policy"],
        "description": "Quality of governance in OECD countries",
        "indicator_count": 193
    },
    "FAO_AS": {
        "name": "Agriculture Statistics",
        "organization": "FAO",
        "themes": ["Agriculture", "Food Security", "Environment"],
        "description": "Agricultural production, trade, and food security indicators",
        "indicator_count": 124
    },
    "FH_FIW": {
        "name": "Freedom in the World",
        "organization": "Freedom House",
        "themes": ["Governance", "Democracy", "Civil Liberties"],
        "description": "Political rights and civil liberties assessments",
        "indicator_count": 38
    },
    "GEM_APS": {
        "name": "Adult Population Survey",
        "organization": "Global Entrepreneurship Monitor",
        "themes": ["Entrepreneurship", "Business", "Innovation"],
        "description": "Entrepreneurial activity and attitudes",
        "indicator_count": 15
    },
    "GEM_NES": {
        "name": "National Expert Survey",
        "organization": "Global Entrepreneurship Monitor",
        "themes": ["Entrepreneurship", "Business"],
        "description": "Expert assessments of entrepreneurship conditions",
        "indicator_count": 12
    },
    "GI_AII": {
        "name": "Global Innovation Index",
        "organization": "Global Innovation Index",
        "themes": ["Innovation", "Technology", "Economy"],
        "description": "Innovation capabilities and results across countries",
        "indicator_count": 114
    },
    "IDB_INFRALATAM": {
        "name": "InfraLatam",
        "organization": "Inter-American Development Bank",
        "themes": ["Infrastructure", "Transportation", "Energy"],
        "description": "Infrastructure investment in Latin America",
        "indicator_count": 3
    },
    "IFC_GB": {
        "name": "Global Business Environment",
        "organization": "IFC",
        "themes": ["Business", "Economy", "Regulation"],
        "description": "Business environment and regulatory quality",
        "indicator_count": 1
    },
    "ILO_EMP": {
        "name": "Employment Statistics",
        "organization": "ILO",
        "themes": ["Employment", "Labor", "Economy"],
        "description": "Labor market statistics including employment, unemployment, and wages",
        "indicator_count": 6
    },
    "IMF_BOP": {
        "name": "Balance of Payments",
        "organization": "IMF",
        "themes": ["Trade", "Finance", "Current Account"],
        "description": "International transactions including trade balance and capital flows",
        "indicator_count": 5209
    },
    "IMF_BOPAGG": {
        "name": "Balance of Payments Aggregates",
        "organization": "IMF",
        "themes": ["Trade", "Finance"],
        "description": "Aggregated balance of payments statistics",
        "indicator_count": 38
    },
    "IMF_CDIR": {
        "name": "Coordinated Direct Investment",
        "organization": "IMF",
        "themes": ["Economy", "Finance", "Investment"],
        "description": "Direct investment positions by country",
        "indicator_count": 4
    },
    "IMF_CDIS": {
        "name": "Coordinated Direct Investment Survey",
        "organization": "IMF",
        "themes": ["Economy", "Finance", "Investment"],
        "description": "Direct investment survey data",
        "indicator_count": 20
    },
    "IMF_CPIS": {
        "name": "Coordinated Portfolio Investment Survey",
        "organization": "IMF",
        "themes": ["Economy", "Finance", "Investment"],
        "description": "Portfolio investment holdings by country",
        "indicator_count": 50
    },
    "IMF_ET": {
        "name": "Exchange Rates",
        "organization": "IMF",
        "themes": ["Economy", "Finance", "Currency"],
        "description": "Exchange rate data and currency statistics",
        "indicator_count": 5
    },
    "IMF_FAS": {
        "name": "Financial Access Survey",
        "organization": "IMF",
        "themes": ["Finance", "Financial Inclusion"],
        "description": "Financial access and inclusion indicators",
        "indicator_count": 82
    },
    "IMF_FFS": {
        "name": "Financial Fragility Survey",
        "organization": "IMF",
        "themes": ["Finance", "Financial Stability"],
        "description": "Financial fragility and stability metrics",
        "indicator_count": 21
    },
    "IMF_FISCALDECENTRALIZATION": {
        "name": "Fiscal Decentralization",
        "organization": "IMF",
        "themes": ["Fiscal", "Government", "Public Finance"],
        "description": "Fiscal decentralization indicators",
        "indicator_count": 36
    },
    "IMF_FSI": {
        "name": "Financial Soundness Indicators",
        "organization": "IMF",
        "themes": ["Finance", "Banking", "Financial Stability"],
        "description": "Banking sector health and stability indicators",
        "indicator_count": 593
    },
    "IMF_FSIRE": {
        "name": "FSI Regulatory",
        "organization": "IMF",
        "themes": ["Finance", "Banking", "Regulation"],
        "description": "Financial soundness regulatory indicators",
        "indicator_count": 21
    },
    "IMF_GFSCOFOG": {
        "name": "Government Finance - COFOG",
        "organization": "IMF",
        "themes": ["Fiscal", "Government"],
        "description": "Government expenditure by function",
        "indicator_count": 80
    },
    "IMF_GFSE": {
        "name": "Government Finance Statistics",
        "organization": "IMF",
        "themes": ["Fiscal", "Government", "Public Finance"],
        "description": "Government revenue, expenditure, and debt",
        "indicator_count": 48
    },
    "IMF_GFSIBS": {
        "name": "GFS Integrated Balance Sheet",
        "organization": "IMF",
        "themes": ["Fiscal", "Government"],
        "description": "Government balance sheet data",
        "indicator_count": 15
    },
    "IMF_GFSMAB": {
        "name": "GFS Main Aggregates",
        "organization": "IMF",
        "themes": ["Fiscal", "Government"],
        "description": "Main government finance aggregates",
        "indicator_count": 70
    },
    "IMF_GFSR": {
        "name": "Global Financial Stability Report",
        "organization": "IMF",
        "themes": ["Finance", "Financial Stability"],
        "description": "Global financial stability assessments",
        "indicator_count": 84
    },
    "IMF_GFSSSUC": {
        "name": "GFS Summary",
        "organization": "IMF",
        "themes": ["Fiscal", "Government"],
        "description": "Summary government finance statistics",
        "indicator_count": 26
    },
    "IMF_IRFCL": {
        "name": "International Reserves and Foreign Currency",
        "organization": "IMF",
        "themes": ["Finance", "Reserves", "Currency"],
        "description": "International reserves and liquidity data",
        "indicator_count": 120
    },
    "IMF_PCTOT": {
        "name": "Primary Commodity Prices",
        "organization": "IMF",
        "themes": ["Economy", "Commodities", "Prices"],
        "description": "Primary commodity price indices",
        "indicator_count": 6
    },
    "IMF_WEO": {
        "name": "World Economic Outlook",
        "organization": "IMF",
        "themes": ["Economy", "GDP", "Fiscal Policy", "Forecasts"],
        "description": "Macroeconomic indicators and projections for 190+ countries",
        "indicator_count": 44
    },
    "ITU_DH": {
        "name": "Digital Health",
        "organization": "ITU",
        "themes": ["Health", "Technology", "Digital"],
        "description": "Digital health technology indicators",
        "indicator_count": 39
    },
    "ITU_GCI": {
        "name": "ICT Development Index",
        "organization": "ITU",
        "themes": ["Technology", "Telecommunications", "Innovation"],
        "description": "ICT development and infrastructure",
        "indicator_count": 26
    },
    "ITU_ICT": {
        "name": "ICT Indicators",
        "organization": "ITU",
        "themes": ["Technology", "Telecommunications", "Digital"],
        "description": "Information and communication technology adoption and access",
        "indicator_count": 10
    },
    "JRC_EDGAR": {
        "name": "Emissions Database",
        "organization": "Joint Research Centre",
        "themes": ["Environment", "Climate", "Emissions"],
        "description": "Greenhouse gas emissions by country and sector",
        "indicator_count": 10
    },
    "OECDWBG_PMR": {
        "name": "Product Market Regulation",
        "organization": "OECD",
        "themes": ["Economy", "Regulation", "Business"],
        "description": "Product market regulation indicators",
        "indicator_count": 33
    },
    "OECD_BROADBAND": {
        "name": "Broadband Statistics",
        "organization": "OECD",
        "themes": ["Technology", "Telecommunications", "Infrastructure"],
        "description": "Broadband penetration and quality metrics",
        "indicator_count": 11
    },
    "OECD_IDD": {
        "name": "International Development Database",
        "organization": "OECD",
        "themes": ["Development", "Aid", "Finance"],
        "description": "Official development assistance and aid flows",
        "indicator_count": 53
    },
    "OECD_TIVA": {
        "name": "Trade in Value Added",
        "organization": "OECD",
        "themes": ["Trade", "Economy", "Value Chains"],
        "description": "Global value chain participation and trade in value added",
        "indicator_count": 24
    },
    "OWID_CB": {
        "name": "Our World in Data",
        "organization": "Our World in Data",
        "themes": ["Research", "Development", "Multiple"],
        "description": "Research-based development indicators across multiple topics",
        "indicator_count": 76
    },
    "POLITY5_PRC": {
        "name": "Polity5 Political Regime",
        "organization": "Center for Systemic Peace",
        "themes": ["Governance", "Political Systems", "Democracy"],
        "description": "Political regime characteristics and transitions",
        "indicator_count": 14
    },
    "RWB_PFI": {
        "name": "Press Freedom Index",
        "organization": "Reporters Without Borders",
        "themes": ["Governance", "Media", "Freedom"],
        "description": "Press freedom and journalist safety",
        "indicator_count": 12
    },
    "UIS_EDSTATS": {
        "name": "UNESCO Education Statistics",
        "organization": "UNESCO Institute for Statistics",
        "themes": ["Education", "Literacy", "Skills"],
        "description": "Detailed education statistics from pre-primary to tertiary",
        "indicator_count": 41
    },
    "UNCTAD_DE": {
        "name": "Development Economics",
        "organization": "UNCTAD",
        "themes": ["Economy", "Development", "Trade"],
        "description": "Economic development and trade indicators",
        "indicator_count": 14
    },
    "UNCTAD_MT": {
        "name": "Maritime Transport",
        "organization": "UNCTAD",
        "themes": ["Trade", "Transportation", "Logistics"],
        "description": "Maritime transport and port statistics",
        "indicator_count": 9
    },
    "UNDRR_SFM": {
        "name": "Sendai Framework Monitor",
        "organization": "UN Office for Disaster Risk Reduction",
        "themes": ["Environment", "Disaster Risk", "Resilience"],
        "description": "Disaster risk reduction indicators",
        "indicator_count": 36
    },
    "UNESCO_UIS": {
        "name": "UNESCO Institute for Statistics",
        "organization": "UNESCO",
        "themes": ["Education", "Science", "Culture", "Communication"],
        "description": "Education, science, culture and communication statistics",
        "indicator_count": 2
    },
    "UNICEF_DW": {
        "name": "UNICEF Data Warehouse",
        "organization": "UNICEF",
        "themes": ["Health", "Education", "Child Welfare", "Nutrition"],
        "description": "Child-focused development indicators covering health, education, and protection",
        "indicator_count": 16
    },
    "UNSD_EI": {
        "name": "Environment Indicators",
        "organization": "UN Statistics Division",
        "themes": ["Environment", "Sustainability"],
        "description": "Environmental and sustainability indicators",
        "indicator_count": 20
    },
    "VDEM_CORE": {
        "name": "Varieties of Democracy",
        "organization": "V-Dem Institute",
        "themes": ["Governance", "Democracy", "Political Rights"],
        "description": "Comprehensive democracy indicators covering electoral, liberal, participatory, deliberative, and egalitarian principles",
        "indicator_count": 84
    },
    "WB_BID": {
        "name": "Business Intelligence Dashboard",
        "organization": "World Bank",
        "themes": ["Business", "Economy"],
        "description": "Business intelligence indicators",
        "indicator_count": 4
    },
    "WB_BOOST": {
        "name": "BOOST Public Expenditure",
        "organization": "World Bank",
        "themes": ["Fiscal", "Government", "Public Finance"],
        "description": "Government expenditure data by sector and economic classification",
        "indicator_count": 232
    },
    "WB_BPS": {
        "name": "Business Pulse Survey",
        "organization": "World Bank",
        "themes": ["Business", "Economy", "COVID-19"],
        "description": "Business impacts from COVID-19 pandemic",
        "indicator_count": 28
    },
    "WB_BREADY": {
        "name": "Business Ready",
        "organization": "World Bank",
        "themes": ["Business", "Regulation"],
        "description": "Business regulatory environment indicators",
        "indicator_count": 3
    },
    "WB_CCDFS": {
        "name": "Climate Change Data and Finance",
        "organization": "World Bank",
        "themes": ["Climate", "Environment", "Finance"],
        "description": "Climate change and finance data",
        "indicator_count": 23
    },
    "WB_CCKP": {
        "name": "Climate Change Knowledge Portal",
        "organization": "World Bank",
        "themes": ["Climate", "Environment"],
        "description": "Climate change projections and historical data",
        "indicator_count": 40
    },
    "WB_CLEAR": {
        "name": "Country Learning and Evaluation",
        "organization": "World Bank",
        "themes": ["Development", "Evaluation"],
        "description": "Country learning and evaluation indicators",
        "indicator_count": 93
    },
    "WB_CPIA": {
        "name": "Country Policy and Institutional Assessment",
        "organization": "World Bank",
        "themes": ["Governance", "Policy", "Institutions"],
        "description": "Policy and institutional quality assessments",
        "indicator_count": 21
    },
    "WB_CSC": {
        "name": "Country Statistical Capacity",
        "organization": "World Bank",
        "themes": ["Statistics", "Data Quality"],
        "description": "Statistical capacity indicators",
        "indicator_count": 64
    },
    "WB_EDSTATS": {
        "name": "Education Statistics",
        "organization": "World Bank",
        "themes": ["Education", "Enrollment", "Literacy"],
        "description": "Comprehensive education statistics including enrollment, completion, and learning outcomes",
        "indicator_count": 1071
    },
    "WB_EQOSOGI": {
        "name": "Equity of Opportunity",
        "organization": "World Bank",
        "themes": ["Social", "Equality", "Opportunity"],
        "description": "Equity and opportunity indicators",
        "indicator_count": 6
    },
    "WB_ES": {
        "name": "Enterprise Surveys",
        "organization": "World Bank",
        "themes": ["Business", "Economy", "Investment"],
        "description": "Firm-level business environment data",
        "indicator_count": 540
    },
    "WB_ESG": {
        "name": "ESG Data",
        "organization": "World Bank",
        "themes": ["Environment", "Social", "Governance"],
        "description": "Environmental, social, and governance indicators",
        "indicator_count": 71
    },
    "WB_EWSA": {
        "name": "Early Warning System",
        "organization": "World Bank",
        "themes": ["Economy", "Crisis", "Risk"],
        "description": "Economic crisis early warning indicators",
        "indicator_count": 29
    },
    "WB_FINDEX": {
        "name": "Global Findex Database",
        "organization": "World Bank",
        "themes": ["Finance", "Financial Inclusion"],
        "description": "Financial inclusion data covering account ownership, payments, savings, and credit",
        "indicator_count": 280
    },
    "WB_FSI": {
        "name": "Financial Sector Indicators",
        "organization": "World Bank",
        "themes": ["Finance", "Banking"],
        "description": "Financial sector development indicators",
        "indicator_count": 63
    },
    "WB_GIRG": {
        "name": "Global Identification Challenge",
        "organization": "World Bank",
        "themes": ["Digital ID", "Governance"],
        "description": "Data on identification systems and coverage",
        "indicator_count": 6
    },
    "WB_GS": {
        "name": "Gender Statistics",
        "organization": "World Bank",
        "themes": ["Gender", "Social", "Equality"],
        "description": "Gender-disaggregated data across demographics, education, health, and economy",
        "indicator_count": 363
    },
    "WB_GTMI": {
        "name": "Global Trade Monitoring",
        "organization": "World Bank",
        "themes": ["Trade", "Economy"],
        "description": "Global trade monitoring indicators",
        "indicator_count": 58
    },
    "WB_HCP": {
        "name": "Human Capital Project",
        "organization": "World Bank",
        "themes": ["Human Capital", "Education", "Health"],
        "description": "Human capital development indicators",
        "indicator_count": 133
    },
    "WB_HNP": {
        "name": "Health Nutrition and Population",
        "organization": "World Bank",
        "themes": ["Health", "Nutrition", "Demographics"],
        "description": "Health system performance, disease prevalence, and demographic indicators",
        "indicator_count": 221
    },
    "WB_LPI": {
        "name": "Logistics Performance Index",
        "organization": "World Bank",
        "themes": ["Trade", "Logistics", "Infrastructure"],
        "description": "Logistics and supply chain performance",
        "indicator_count": 18
    },
    "WB_MPO": {
        "name": "Macro Poverty Outlook",
        "organization": "World Bank",
        "themes": ["Poverty", "Economy", "Forecasts"],
        "description": "Poverty and economic outlook projections",
        "indicator_count": 103
    },
    "WB_RISE": {
        "name": "Regulatory Indicators for Sustainable Energy",
        "organization": "World Bank",
        "themes": ["Energy", "Regulation", "Sustainability"],
        "description": "Sustainable energy regulatory framework",
        "indicator_count": 38
    },
    "WB_SPI": {
        "name": "Statistical Performance Indicators",
        "organization": "World Bank",
        "themes": ["Statistics", "Data Quality"],
        "description": "Statistical performance and capacity indicators",
        "indicator_count": 71
    },
    "WB_SSGD": {
        "name": "Subnational Statistics on Gender",
        "organization": "World Bank",
        "themes": ["Gender", "Social", "Subnational"],
        "description": "Subnational gender statistics",
        "indicator_count": 128
    },
    "WB_THINK_HAZARD": {
        "name": "ThinkHazard",
        "organization": "World Bank",
        "themes": ["Environment", "Disaster Risk", "Hazards"],
        "description": "Natural hazard risk information",
        "indicator_count": 11
    },
    "WB_WBL": {
        "name": "Women Business and the Law",
        "organization": "World Bank",
        "themes": ["Gender", "Business", "Legal Rights"],
        "description": "Gender equality in business and legal rights",
        "indicator_count": 49
    },
    "WB_WDI": {
        "name": "World Development Indicators",
        "organization": "World Bank",
        "themes": ["Economy", "Demographics", "Education", "Health", "Environment", "Infrastructure"],
        "description": "Primary World Bank database with 1500+ indicators covering all aspects of development",
        "indicator_count": 1508
    },
    "WB_WGI": {
        "name": "Worldwide Governance Indicators",
        "organization": "World Bank",
        "themes": ["Governance", "Political Stability", "Rule of Law"],
        "description": "Governance quality indicators across 200+ countries since 1996",
        "indicator_count": 36
    },
    "WB_WITS": {
        "name": "World Integrated Trade Solution",
        "organization": "World Bank",
        "themes": ["Trade", "Economy", "Tariffs"],
        "description": "International trade statistics, tariffs, and trade agreements",
        "indicator_count": 44
    },
    "WB_WWBI": {
        "name": "Worldwide Bureaucracy Indicators",
        "organization": "World Bank",
        "themes": ["Governance", "Public Sector"],
        "description": "Public sector workforce and bureaucracy indicators",
        "indicator_count": 37
    },
    "WEF_GCI": {
        "name": "Global Competitiveness Index",
        "organization": "World Economic Forum",
        "themes": ["Economy", "Competitiveness", "Innovation", "Infrastructure"],
        "description": "National competitiveness across 12 pillars including innovation and institutions",
        "indicator_count": 169
    },
    "WEF_GCIHH": {
        "name": "Global Competitiveness Index (Historical)",
        "organization": "World Economic Forum",
        "themes": ["Economy", "Competitiveness", "Innovation"],
        "description": "Historical global competitiveness data",
        "indicator_count": 163
    },
    "WEF_TTDI": {
        "name": "Travel & Tourism Development Index",
        "organization": "World Economic Forum",
        "themes": ["Tourism", "Economy", "Infrastructure"],
        "description": "Tourism competitiveness and development",
        "indicator_count": 140
    },
    "WI_GRT": {
        "name": "Global Inequality Database",
        "organization": "World Inequality Database",
        "themes": ["Social", "Inequality", "Income"],
        "description": "Income and wealth inequality data",
        "indicator_count": 4
    },
    "WJP_ROL": {
        "name": "Rule of Law Index",
        "organization": "World Justice Project",
        "themes": ["Governance", "Justice", "Rule of Law"],
        "description": "Rule of law performance across multiple dimensions",
        "indicator_count": 53
    },
    "WRI_CLIMATEWATCH": {
        "name": "Climate Watch",
        "organization": "World Resources Institute",
        "themes": ["Climate", "Environment", "Policy"],
        "description": "Climate change data and policy tracking",
        "indicator_count": 2
    },
}

THEME_TAXONOMY = {
    "Economy": ["GDP", "Growth", "Trade", "Investment", "Employment", "Productivity", "Fiscal Policy"],
    "Demographics": ["Population", "Migration", "Urbanization", "Age Structure", "Vital Statistics"],
    "Education": ["Enrollment", "Literacy", "Completion", "Quality", "Skills", "Learning Outcomes"],
    "Health": ["Mortality", "Diseases", "Healthcare Access", "Nutrition", "Reproductive Health"],
    "Environment": ["Climate", "Emissions", "Energy", "Water", "Biodiversity", "Pollution"],
    "Governance": ["Democracy", "Corruption", "Rule of Law", "Stability", "Political Rights", "Civil Liberties"],
    "Infrastructure": ["Transportation", "Telecommunications", "Energy", "Water", "Digital"],
    "Finance": ["Banking", "Financial Inclusion", "Investment", "Financial Stability", "Markets"],
    "Social": ["Poverty", "Inequality", "Gender", "Social Protection", "Human Capital"],
    "Agriculture": ["Production", "Food Security", "Rural Development"],
    "Technology": ["ICT", "Innovation", "Digital Infrastructure", "R&D"],
    "Trade": ["Exports", "Imports", "Tariffs", "Value Chains", "Logistics"],
    "Business": ["Entrepreneurship", "Regulation", "Investment Climate"],
    "Climate": ["Emissions", "Adaptation", "Mitigation", "Risk"],
    "Development": ["Aid", "Assistance", "Capacity Building"]
}

COMMON_COUNTRIES = {
    # G20 Countries
    "USA": "United States",
    "CHN": "China",
    "JPN": "Japan",
    "DEU": "Germany",
    "GBR": "United Kingdom",
    "FRA": "France",
    "IND": "India",
    "ITA": "Italy",
    "BRA": "Brazil",
    "CAN": "Canada",
    "KOR": "South Korea",
    "RUS": "Russia",
    "AUS": "Australia",
    "ESP": "Spain",
    "MEX": "Mexico",
    "IDN": "Indonesia",
    "NLD": "Netherlands",
    "SAU": "Saudi Arabia",
    "TUR": "Turkey",
    "ARG": "Argentina",
    "ZAF": "South Africa",
    
    # Other Major Economies
    "CHE": "Switzerland",
    "POL": "Poland",
    "BEL": "Belgium",
    "SWE": "Sweden",
    "NOR": "Norway",
    "AUT": "Austria",
    "DNK": "Denmark",
    "FIN": "Finland",
    "IRL": "Ireland",
    "SGP": "Singapore",
    "HKG": "Hong Kong",
    "MYS": "Malaysia",
    "THA": "Thailand",
    "PHL": "Philippines",
    "VNM": "Vietnam",
    "EGY": "Egypt",
    "NGA": "Nigeria",
    "PAK": "Pakistan",
    "BGD": "Bangladesh",
    "IRN": "Iran",
    "ARE": "United Arab Emirates",
    "ISR": "Israel",
    "CHL": "Chile",
    "COL": "Colombia",
    "PER": "Peru",
    
    # African Countries
    "KEN": "Kenya",
    "ETH": "Ethiopia",
    "GHA": "Ghana",
    "TZA": "Tanzania",
    "UGA": "Uganda",
    "MAR": "Morocco",
    "DZA": "Algeria",
    "TUN": "Tunisia",
    
    # European Countries
    "CZE": "Czech Republic",
    "PRT": "Portugal",
    "GRC": "Greece",
    "HUN": "Hungary",
    "ROU": "Romania",
    "UKR": "Ukraine",
    
    # Latin American Countries
    "VEN": "Venezuela",
    "ECU": "Ecuador",
    "GTM": "Guatemala",
    "CRI": "Costa Rica",
    "URY": "Uruguay",
    "PAN": "Panama",
    
    # Asia Pacific
    "NZL": "New Zealand",
    "LKA": "Sri Lanka",
    "NPL": "Nepal",
    "KHM": "Cambodia",
    "MMR": "Myanmar"
}

REGIONS = {
    "EAS": "East Asia & Pacific",
    "ECS": "Europe & Central Asia",
    "LCN": "Latin America & Caribbean",
    "MEA": "Middle East & North Africa",
    "NAC": "North America",
    "SAS": "South Asia",
    "SSF": "Sub-Saharan Africa",
    "WLD": "World"
}

INCOME_GROUPS = {
    "HIC": "High Income",
    "UMC": "Upper Middle Income",
    "LMC": "Lower Middle Income",
    "LIC": "Low Income"
}


# ============================================================================
# CACHING FUNCTIONS
# ============================================================================

@st.cache_data(ttl=3600)
def discover_databases():
    """Cached database discovery - returns known list of databases"""
    known_databases = [
        'BS_BTI', 'BS_SGI', 'FAO_AS', 'FH_FIW', 'GEM_APS', 'GEM_NES', 'GI_AII', 
        'IDB_INFRALATAM', 'IFC_GB', 'ILO_EMP', 'IMF_BOP', 'IMF_BOPAGG', 'IMF_CDIR', 
        'IMF_CDIS', 'IMF_CPIS', 'IMF_ET', 'IMF_FAS', 'IMF_FFS', 'IMF_FISCALDECENTRALIZATION', 
        'IMF_FSI', 'IMF_FSIRE', 'IMF_GFSCOFOG', 'IMF_GFSE', 'IMF_GFSIBS', 'IMF_GFSMAB', 
        'IMF_GFSR', 'IMF_GFSSSUC', 'IMF_IFS', 'IMF_IRFCL', 'IMF_PCTOT', 'IMF_WEO', 
        'ITU_DH', 'ITU_GCI', 'ITU_ICT', 'JRC_EDGAR', 'OECDWBG_PMR', 'OECD_BROADBAND', 
        'OECD_IDD', 'OECD_TIVA', 'OWID_CB', 'POLITY5_PRC', 'RWB_PFI', 'UIS_EDSTATS', 
        'UNCTAD_DE', 'UNCTAD_MT', 'UNDRR_SFM', 'UNESCO_UIS', 'UNICEF_DW', 'UNSD_EI', 
        'VDEM_CORE', 'WB_BID', 'WB_BOOST', 'WB_BPS', 'WB_BREADY', 'WB_CCDFS', 'WB_CCKP', 
        'WB_CLEAR', 'WB_CPIA', 'WB_CSC', 'WB_EDSTATS', 'WB_EQOSOGI', 'WB_ES', 'WB_ESG', 
        'WB_EWSA', 'WB_FINDEX', 'WB_FSI', 'WB_GIRG', 'WB_GS', 'WB_GTMI', 'WB_HCP', 
        'WB_HNP', 'WB_IDS', 'WB_LPI', 'WB_MPO', 'WB_RISE', 'WB_SPI', 'WB_SSGD', 
        'WB_THINK_HAZARD', 'WB_WBL', 'WB_WDI', 'WB_WGI', 'WB_WITS', 'WB_WWBI', 
        'WEF_GCI', 'WEF_GCIHH', 'WEF_TTDI', 'WI_GRT', 'WJP_ROL', 'WRI_CLIMATEWATCH'
    ]
    
    try:
        client = Data360Client()
        result = client.search("population", top=1000)
        
        if "value" in result:
            api_databases = set()
            for item in result["value"]:
                if "series_description" in item:
                    db_id = item["series_description"].get("database_id")
                    if db_id:
                        api_databases.add(db_id)
            
            if api_databases:
                all_databases = set(known_databases) | api_databases
                return sorted(list(all_databases))
    except:
        pass
    
    return sorted(known_databases)


# Common abbreviation decoder for better readability
ABBREVIATION_DECODER = {
    # IMF Balance of Payments
    "BOP": "Balance of Payments",
    "BP6": "BPM6",
    "BXSTR": "External Debt",
    "BXSTV": "External Debt Value",
    "SPE": "Special",
    "USD": "US Dollars",
    "EUR": "Euros",
    "GBP": "British Pounds",
    "JPY": "Japanese Yen",
    "XDC": "SDR (Special Drawing Rights)",
    "IARMGB": "Interest Arrears",
    "BFOCDLOF": "Foreign Direct Investment",
    "BMGN": "Monetary Gold",
    "BXSTVBS": "External Debt by Sector",
    "BIPIOPC": "Portfolio Investment",
    "BFOLN": "Financial Derivatives",
    "BFOINLG": "Foreign Investment",
    "BFQINLG": "Financial Account",
    "BFQIN": "Financial Account Inflows",
    
    # World Bank
    "WDI": "World Development Indicators",
    "AG": "Agriculture",
    "CON": "Consumption",
    "FERT": "Fertilizer",
    "ZS": "% of",
    "NY": "National Accounts",
    "GDP": "Gross Domestic Product",
    "MKTP": "Market Prices",
    "CD": "Current USD",
    "PCAP": "Per Capita",
    "KD": "Constant USD",
    "SP": "Population",
    "POP": "Population",
    "TOTL": "Total",
    "FP": "Prices",
    "CPI": "Consumer Price Index",
    "SL": "Labor",
    "UEM": "Unemployment",
    "SE": "Education",
    "PRM": "Primary",
    "CMPT": "Completion Rate",
    "FE": "Female",
    "MA": "Male",
    "SH": "Health",
    "DYN": "Dynamics",
    "LE00": "Life Expectancy",
    "IN": "Indicator",
    
    # Common terms
    "PCT": "Percent",
    "TOT": "Total",
    "AVG": "Average",
    "IDX": "Index",
    "IND": "Industry",
    "MFG": "Manufacturing",
    "SVC": "Services",
    "GOVT": "Government",
    "EXP": "Exports",
    "IMP": "Imports",
    "INV": "Investment",
    "FDI": "Foreign Direct Investment",
    "GNI": "Gross National Income",
    "PPP": "Purchasing Power Parity"
}


def decode_indicator_name(indicator_id: str, raw_name: str = None) -> str:
    """
    Decode cryptic indicator IDs into readable names
    """
    # If we have a proper name from the API, use it
    if raw_name and len(raw_name) > 20 and not raw_name.isupper():
        return raw_name
    
    # Otherwise, try to decode the ID
    # Remove database prefix
    parts = indicator_id.split("_")
    
    # Skip database prefix (first 1-2 parts usually)
    meaningful_parts = []
    for i, part in enumerate(parts):
        if i < 2:  # Skip database prefix
            continue
        
        # Try to decode each part
        decoded = ABBREVIATION_DECODER.get(part.upper(), part)
        meaningful_parts.append(decoded)
    
    if meaningful_parts:
        decoded_name = " - ".join(meaningful_parts)
        return decoded_name
    
    # Fallback: just make it readable
    return raw_name or indicator_id.replace("_", " ").title()


@st.cache_data(ttl=3600)
def get_indicators_with_metadata(database_id: str, limit: int = 500):
    """Get indicators with their metadata - uses search API to get full metadata"""
    client = Data360Client()
    
    try:
        # First, try using the search API to get rich metadata
        # Search for all indicators in this database
        filter_str = f"series_description/database_id eq '{database_id}'"
        result = client.search("*", top=min(limit, 1000), filter_by=filter_str)
        
        indicators = []
        if "value" in result and result["value"]:
            for item in result["value"]:
                desc = item.get("series_description", {})
                if desc.get("idno"):
                    raw_name = desc.get("name", "")
                    indicator_id = desc.get("idno")
                    
                    # Use API name if good, otherwise decode
                    if raw_name and len(raw_name) > 20 and not raw_name.isupper():
                        display_name = raw_name
                    else:
                        display_name = decode_indicator_name(indicator_id, raw_name)
                    
                    indicators.append({
                        "id": indicator_id,
                        "name": display_name,
                        "description": desc.get("description", ""),
                        "topics": [t.get("name", "") for t in desc.get("topics", [])],
                        "source": desc.get("source", {}),
                        "database_id": database_id
                    })
            
            if indicators:
                return indicators[:limit]
        
        # Fallback: use /indicators endpoint if search fails
        indicator_ids = client.list_indicators(database_id)
        
        indicators = []
        for ind_id in indicator_ids[:limit]:
            # Create a more readable name from the ID
            display_name = decode_indicator_name(ind_id)
            indicators.append({
                "id": ind_id,
                "name": display_name,
                "description": f"Indicator from {DATABASE_CATALOG.get(database_id, {}).get('name', database_id)}",
                "topics": [],
                "source": {},
                "database_id": database_id
            })
        
        return indicators
    except Exception as e:
        st.error(f"Could not load indicators for {database_id}: {str(e)}")
        return []


@st.cache_data(ttl=3600)
def search_indicators_filtered(query: str, themes: List[str] = None, 
                               databases: List[str] = None, 
                               organizations: List[str] = None,
                               limit: int = 100):
    """Search with filters including organizations"""
    client = Data360Client()
    
    if not query or query.strip() == "" or query == "*":
        query = "GDP"
    
    filters = []
    
    # Build database filter from organizations or direct database selection
    if organizations:
        org_databases = [
            db_id for db_id, info in DATABASE_CATALOG.items()
            if info['organization'] in organizations
        ]
        if org_databases:
            db_filter = " or ".join([f"series_description/database_id eq '{db}'" for db in org_databases])
            filters.append(f"({db_filter})")
    elif databases:
        db_filter = " or ".join([f"series_description/database_id eq '{db}'" for db in databases])
        filters.append(f"({db_filter})")
    
    # Add theme filter if specified
    if themes:
        theme_filter = " or ".join([f"series_description/topics/any(t: t/name eq '{theme}')" for theme in themes])
        filters.append(f"({theme_filter})")
    
    filter_str = " and ".join(filters) if filters else None
    
    try:
        result = client.search(query, top=limit, filter_by=filter_str)
        
        indicators = []
        if "value" in result and result["value"]:
            for item in result["value"]:
                desc = item.get("series_description", {})
                # Only include items that have an idno (indicators)
                if desc.get("idno"):
                    indicators.append({
                        "id": desc.get("idno"),
                        "name": desc.get("name"),
                        "description": desc.get("description", ""),
                        "topics": [t.get("name", "") for t in desc.get("topics", [])],
                        "database_id": desc.get("database_id"),
                        "source": desc.get("source", {})
                    })
        
        return indicators, result.get("@odata.count", len(indicators))
    except Exception as e:
        # Show actual error for debugging
        import traceback
        error_msg = str(e)
        st.error(f"Search failed: {error_msg}")
        st.code(traceback.format_exc())
        return [], 0


@st.cache_data(ttl=3600)
def fetch_data_cached(database_id: str, indicator: str, countries: List[str], 
                     year_from: str, year_to: str):
    """Cached data fetch for multiple countries"""
    client = Data360Client()
    all_data = []
    
    for country in countries:
        try:
            data = client.get_data(
                database_id=database_id,
                indicator=indicator,
                ref_area=country,
                time_period_from=year_from,
                time_period_to=year_to,
                auto_paginate=True,
                max_records=5000
            )
            all_data.extend(data.get("value", []))
        except Exception:
            pass
    
    return all_data


# ============================================================================
# VISUALIZATION FUNCTIONS
# ============================================================================

def create_time_series_plot(df: pd.DataFrame, title: str, indicator_name: str = ""):
    """Create elegant time series chart"""
    fig = go.Figure()
    
    colors = px.colors.qualitative.Set2
    
    for idx, country in enumerate(df['REF_AREA'].unique()):
        country_data = df[df['REF_AREA'] == country].sort_values('TIME_PERIOD')
        country_name = COMMON_COUNTRIES.get(country, country)
        
        fig.add_trace(go.Scatter(
            x=country_data['TIME_PERIOD'],
            y=country_data['OBS_VALUE'],
            name=country_name,
            mode='lines+markers',
            line=dict(width=3, color=colors[idx % len(colors)]),
            marker=dict(size=7, symbol='circle'),
            hovertemplate=f'<b>{country_name}</b><br>Year: %{{x}}<br>Value: %{{y:.2f}}<extra></extra>'
        ))
    
    fig.update_layout(
        title=dict(
            text=f"<b>{title}</b><br><sub>{indicator_name}</sub>",
            font=dict(size=20, color='#ffffff'),
            x=0.5,
            xanchor='center'
        ),
        xaxis_title="Year",
        yaxis_title="Value",
        template="plotly_dark",
        hovermode='x unified',
        plot_bgcolor='#1a1d29',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#ffffff', size=12),
        legend=dict(
            orientation="v",
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=1.02,
            bgcolor='rgba(30, 33, 48, 0.8)',
            bordercolor='#2d3142',
            borderwidth=1
        ),
        margin=dict(l=60, r=150, t=80, b=60),
        height=500
    )
    
    return fig


def create_comparison_chart(df: pd.DataFrame, year: str, title: str):
    """Create bar chart for country comparison"""
    year_data = df[df['TIME_PERIOD'] == year].copy()
    year_data['Country'] = year_data['REF_AREA'].map(lambda x: COMMON_COUNTRIES.get(x, x))
    year_data = year_data.sort_values('OBS_VALUE', ascending=True)
    
    fig = go.Figure(go.Bar(
        x=year_data['OBS_VALUE'],
        y=year_data['Country'],
        orientation='h',
        marker=dict(
            color=year_data['OBS_VALUE'],
            colorscale='Viridis',
            showscale=True,
            line=dict(color='#2d3142', width=1)
        ),
        hovertemplate='<b>%{y}</b><br>Value: %{x:.2f}<extra></extra>'
    ))
    
    fig.update_layout(
        title=dict(
            text=f"<b>{title}</b><br><sub>Year: {year}</sub>",
            font=dict(size=20, color='#ffffff'),
            x=0.5,
            xanchor='center'
        ),
        xaxis_title="Value",
        yaxis_title="",
        template="plotly_dark",
        plot_bgcolor='#1a1d29',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#ffffff', size=12),
        height=max(400, len(year_data) * 30),
        margin=dict(l=150, r=60, t=80, b=60)
    )
    
    return fig


# ============================================================================
# STREAMLIT APP CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="Data360 Explorer",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS - Dark Mode Elegant Design
st.markdown("""
    <style>
    /* Main background */
    .main {
        background: linear-gradient(135deg, #0e1117 0%, #1a1d29 100%);
    }
    
    .stApp {
        background: linear-gradient(135deg, #0e1117 0%, #1a1d29 100%);
    }
    
    /* Headers */
    h1, h2, h3 {
        color: #ffffff;
        font-weight: 300;
        letter-spacing: 0.5px;
    }
    
    h1 {
        font-size: 2.5rem !important;
        margin-bottom: 0.5rem !important;
    }
    
    /* Metric cards */
    div[data-testid="stMetricValue"] {
        font-size: 28px;
        color: #4a9eff;
        font-weight: 500;
    }
    
    div[data-testid="stMetricLabel"] {
        color: #a0a0a0;
        font-size: 14px;
    }
    
    /* Expander styling */
    .streamlit-expanderHeader {
        background: linear-gradient(90deg, #1e2130 0%, #262b3d 100%);
        border-radius: 8px;
        border: 1px solid #2d3142;
        color: #ffffff;
        font-weight: 500;
    }
    
    .streamlit-expanderContent {
        background: #1a1d29;
        border: 1px solid #2d3142;
        border-top: none;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: transparent;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: #1e2130;
        border-radius: 8px 8px 0 0;
        padding: 12px 24px;
        color: #a0a0a0;
        border: 1px solid #2d3142;
        border-bottom: none;
    }
    
    .stTabs [aria-selected="true"] {
        background: #262b3d;
        color: #4a9eff;
        border-color: #4a9eff;
    }
    
    /* Buttons */
    .stButton > button {
        background: linear-gradient(135deg, #4a9eff 0%, #3d7dd4 100%);
        color: white;
        border: none;
        border-radius: 6px;
        padding: 0.5rem 1rem;
        font-weight: 500;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        background: linear-gradient(135deg, #5aa9ff 0%, #4d8de4 100%);
        box-shadow: 0 4px 12px rgba(74, 158, 255, 0.3);
        transform: translateY(-2px);
    }
    
    /* Input fields */
    .stTextInput > div > div > input,
    .stSelectbox > div > div > select,
    .stMultiSelect > div > div > div {
        background: #1e2130;
        border: 1px solid #2d3142;
        border-radius: 6px;
        color: #ffffff;
    }
    
    /* Checkboxes */
    .stCheckbox {
        color: #ffffff;
    }
    
    /* DataFrames */
    .dataframe {
        background: #1a1d29;
        color: #ffffff;
    }
    
    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1a1d29 0%, #1e2130 100%);
        border-right: 1px solid #2d3142;
    }
    
    section[data-testid="stSidebar"] .stMarkdown {
        color: #ffffff;
    }
    
    /* Download button */
    .stDownloadButton > button {
        background: linear-gradient(135deg, #2ecc71 0%, #27ae60 100%);
        color: white;
        border: none;
        border-radius: 6px;
        padding: 0.5rem 1rem;
        font-weight: 500;
    }
    
    /* Info boxes */
    .stAlert {
        background: #1e2130;
        border: 1px solid #2d3142;
        border-radius: 8px;
        color: #ffffff;
    }
    
    /* Filter section cards */
    .filter-card {
        background: linear-gradient(135deg, #1e2130 0%, #262b3d 100%);
        padding: 20px;
        border-radius: 12px;
        border: 1px solid #2d3142;
        margin: 10px 0;
    }
    
    /* Multiselect styling */
    .stMultiSelect > div > div {
        background: #1e2130;
        border: 1px solid #2d3142;
        border-radius: 6px;
    }
    
    /* Search input in main area */
    div[data-testid="stTextInput"] > div > div > input {
        background: #1e2130;
        border: 1px solid #4a9eff;
        border-radius: 8px;
        padding: 12px;
        color: #ffffff;
        font-size: 16px;
    }
    
    div[data-testid="stTextInput"] > div > div > input:focus {
        border-color: #667eea;
        box-shadow: 0 0 0 3px rgba(74, 158, 255, 0.2);
    }
    
    /* Tags/badges */
    .tag {
        display: inline-block;
        background: #2d3142;
        color: #4a9eff;
        padding: 4px 12px;
        border-radius: 12px;
        font-size: 12px;
        margin: 2px;
        border: 1px solid #4a9eff;
    }
    
    .database-badge {
        display: inline-block;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 4px 12px;
        border-radius: 12px;
        font-size: 11px;
        font-weight: 600;
        margin: 2px;
    }
    
    /* Scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    
    ::-webkit-scrollbar-track {
        background: #1a1d29;
    }
    
    ::-webkit-scrollbar-thumb {
        background: #2d3142;
        border-radius: 4px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: #4a9eff;
    }
    </style>
""", unsafe_allow_html=True)


# ============================================================================
# SESSION STATE INITIALIZATION
# ============================================================================

if 'client' not in st.session_state:
    st.session_state.client = Data360Client()

if 'databases' not in st.session_state:
    st.session_state.databases = None

if 'selected_themes' not in st.session_state:
    st.session_state.selected_themes = []

if 'selected_databases' not in st.session_state:
    st.session_state.selected_databases = []

if 'selected_organizations' not in st.session_state:
    st.session_state.selected_organizations = []

if 'current_data' not in st.session_state:
    st.session_state.current_data = None

if 'query_database' not in st.session_state:
    st.session_state.query_database = 'WB_WDI'

if 'show_query_tab' not in st.session_state:
    st.session_state.show_query_tab = False

if 'last_search_results' not in st.session_state:
    st.session_state.last_search_results = None


# ============================================================================
# INITIALIZATION
# ============================================================================

if st.session_state.databases is None:
    with st.spinner("🔄 Loading databases..."):
        st.session_state.databases = discover_databases()


# ============================================================================
# HEADER & FILTERS
# ============================================================================

col1, col2, col3 = st.columns([1, 3, 1])
with col2:
    st.markdown("""
        <div style='text-align: center; padding: 20px 0;'>
            <h1 style='font-size: 3rem; margin-bottom: 0; background: linear-gradient(90deg, #4a9eff 0%, #667eea 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent;'>
                🌍 Data360 Explorer
            </h1>
            <p style='color: #888; font-size: 1.1rem; margin-top: 10px;'>
                Discover insights from 89 global databases • 13,607 indicators
            </p>
        </div>
    """, unsafe_allow_html=True)

# ============================================================================
# BEAUTIFUL FILTER SECTION
# ============================================================================

st.markdown("""
    <div style='text-align: center; margin: 30px 0;'>
        <h2 style='color: #4a9eff; font-weight: 300; letter-spacing: 1px;'>🎯 Filters & Search</h2>
    </div>
""", unsafe_allow_html=True)

st.markdown("<div style='margin: 20px 0;'>", unsafe_allow_html=True)
filter_col1, filter_col2, filter_col3, filter_col4 = st.columns([3, 2, 2, 1])

with filter_col1:
    st.markdown("#### 🔍 Quick Search")
    main_search = st.text_input(
        "Search datasets or indicators",
        placeholder="e.g., GDP, education, climate change...",
        key="main_search_input",
        label_visibility="collapsed"
    )

with filter_col2:
    st.markdown("#### 📚 Themes")
    theme_counts = defaultdict(int)
    for db_id, info in DATABASE_CATALOG.items():
        for theme in info.get("themes", []):
            theme_counts[theme] += 1
    
    theme_options = sorted(theme_counts.keys())
    selected_themes_new = st.multiselect(
        "Select themes",
        options=theme_options,
        default=st.session_state.selected_themes,
        format_func=lambda x: f"{x} ({theme_counts[x]})",
        key="themes_multiselect",
        label_visibility="collapsed"
    )
    st.session_state.selected_themes = selected_themes_new

with filter_col3:
    st.markdown("#### 🏢 Organizations")
    org_counts = defaultdict(int)
    for db_id, info in DATABASE_CATALOG.items():
        org = info.get("organization", "Unknown")
        org_counts[org] += 1
    
    org_options = sorted(org_counts.keys())
    selected_orgs_new = st.multiselect(
        "Select organizations",
        options=org_options,
        default=st.session_state.get('selected_organizations', []),
        format_func=lambda x: f"{x} ({org_counts[x]})",
        key="orgs_multiselect",
        label_visibility="collapsed"
    )
    st.session_state.selected_organizations = selected_orgs_new

with filter_col4:
    st.markdown("#### 🔄")
    if st.button("Clear All", use_container_width=True, key="clear_filters_main"):
        st.session_state.selected_themes = []
        st.session_state.selected_organizations = []
        st.rerun()

st.markdown("</div>", unsafe_allow_html=True)

if st.session_state.selected_themes or st.session_state.get('selected_organizations'):
    st.markdown("<div style='margin: 15px 0;'>", unsafe_allow_html=True)
    st.markdown("**🏷️ Active Filters:**")
    filter_html = ""
    for theme in st.session_state.selected_themes:
        filter_html += f'<span class="tag">{theme}</span>'
    for org in st.session_state.get('selected_organizations', []):
        filter_html += f'<span class="database-badge">{org}</span>'
    st.markdown(filter_html, unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown("""
    <div style='text-align: center; margin: 30px 0 20px 0;'>
        <div style='height: 2px; background: linear-gradient(90deg, transparent, #4a9eff, transparent);'></div>
    </div>
""", unsafe_allow_html=True)

stat_col1, stat_col2, stat_col3, stat_col4, stat_col5 = st.columns(5)
with stat_col1:
    st.metric("📊 Databases", len(st.session_state.databases))
with stat_col2:
    st.metric("📈 Indicators", "13,607")
with stat_col3:
    st.metric("🏢 Organizations", "27")
with stat_col4:
    active_filters = len(st.session_state.selected_themes) + len(st.session_state.get('selected_organizations', []))
    st.metric("🎯 Active Filters", active_filters)
with stat_col5:
    filtered_count = 0
    for db_id, info in DATABASE_CATALOG.items():
        if st.session_state.get('selected_organizations') and info['organization'] not in st.session_state.selected_organizations:
            continue
        if st.session_state.selected_themes:
            if not any(theme in info.get('themes', []) for theme in st.session_state.selected_themes):
                continue
        filtered_count += 1
    st.metric("📁 Showing", filtered_count)

st.markdown("""
    <div style='text-align: center; margin: 20px 0 30px 0;'>
        <div style='height: 2px; background: linear-gradient(90deg, transparent, #667eea, transparent);'></div>
    </div>
""", unsafe_allow_html=True)


# ============================================================================
# MAIN CONTENT - TABS
# ============================================================================

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🗂️ Browse Datasets",
    "🔎 Explore Indicators",
    "📊 Query & Visualize", 
    "🗄️ Database Catalog",
    "💾 Batch Download"
])


# ============================================================================
# TAB 1: BROWSE DATASETS
# ============================================================================

with tab1:
    st.markdown("## Browse Datasets")
    
    # ========================================================================
    # SHOW INDICATORS FIRST IF EXPLORING A DATABASE
    # ========================================================================
    if 'exploring_database' in st.session_state and st.session_state.exploring_database:
        # Big visual indicator that we're in exploration mode
        st.markdown("""
            <div style='background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                        padding: 20px; border-radius: 10px; margin-bottom: 20px;'>
                <h2 style='color: white; margin: 0;'>🔍 Exploring Indicators</h2>
            </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"### {st.session_state.get('exploring_db_name', 'Database')}")
        
        col1, col2 = st.columns([4, 1])
        with col1:
            db_info = DATABASE_CATALOG.get(st.session_state.exploring_database, {})
            st.info(f"📊 Database: `{st.session_state.exploring_database}` | 🏢 {db_info.get('organization', 'Unknown')}")
        with col2:
            if st.button("❌ Close Explorer", key="close_explore", use_container_width=True, type="secondary"):
                del st.session_state.exploring_database
                del st.session_state.exploring_db_name
                st.rerun()
        
        with st.spinner(f"🔄 Loading indicators from {st.session_state.exploring_database}... This may take a moment."):
            explore_indicators = get_indicators_with_metadata(st.session_state.exploring_database, limit=200)
            
            if explore_indicators:
                st.success(f"✅ Loaded {len(explore_indicators)} indicators")
                
                # Search within these indicators
                col1, col2 = st.columns([3, 1])
                with col1:
                    indicator_search = st.text_input(
                        "🔎 Filter indicators by name or ID",
                        placeholder="e.g., population, GDP, health...",
                        key="explore_indicator_search"
                    )
                with col2:
                    st.metric("Total", len(explore_indicators))
                
                # Filter indicators
                if indicator_search:
                    search_lower = indicator_search.lower()
                    filtered_explore = [
                        ind for ind in explore_indicators 
                        if search_lower in ind['name'].lower() 
                        or search_lower in ind['id'].lower()
                        or (ind.get('description') and search_lower in ind['description'].lower())
                    ]
                else:
                    filtered_explore = explore_indicators[:50]  # Show first 50
                
                if indicator_search and not filtered_explore:
                    st.warning(f"No indicators match '{indicator_search}'. Try a different search term.")
                    filtered_explore = explore_indicators[:20]  # Show first 20 as fallback
                
                st.markdown(f"### Showing {len(filtered_explore)} indicators")
                
                for ind in filtered_explore:
                    with st.container():
                        col1, col2 = st.columns([5, 1])
                        
                        with col1:
                            # Show name prominently
                            st.markdown(f"**{ind['name']}**")
                            
                            # Show ID in smaller text
                            st.caption(f"🆔 `{ind['id']}`")
                            
                            # Show description if available
                            if ind.get('description') and ind['description'] != f"Indicator from {st.session_state.exploring_database}":
                                desc_text = ind['description'][:200] + "..." if len(ind.get('description', '')) > 200 else ind.get('description', '')
                                st.markdown(f"*{desc_text}*")
                            
                            # Show topics/tags if available
                            if ind.get('topics') and any(ind.get('topics', [])):
                                topics_html = " ".join([f'<span class="tag">{t}</span>' for t in ind['topics'][:3] if t])
                                st.markdown(topics_html, unsafe_allow_html=True)
                        
                        with col2:
                            if st.button("📊 Query", key=f"explore_query_{ind['id']}", use_container_width=True):
                                st.session_state.selected_indicator = ind
                                st.session_state.query_database = st.session_state.exploring_database
                                # Show a toast message
                                st.toast(f"✅ Selected: {ind['name'][:50]}...", icon="✅")
                                st.info("💡 **Next step:** Go to the '📊 Query & Visualize' tab to fetch and visualize this data!")
                                # Scroll indicator to top by rerunning
                                time.sleep(1)
                        
                        st.markdown("---")
            else:
                st.warning("⚠️ No indicators could be loaded from this database.")
                st.info("This might be because:")
                st.markdown("""
                - The database ID is incorrect
                - The API is temporarily unavailable
                - The database has restricted access
                
                **Try another database from the list below.**
                """)
        
        st.markdown("---")
        st.markdown("""
            <div style='text-align: center; margin: 30px 0;'>
                <h3 style='color: #888;'>⬇️ Scroll down to browse other databases ⬇️</h3>
            </div>
        """, unsafe_allow_html=True)
        st.markdown("---")
    else:
        st.markdown("Explore available datasets with detailed information")
    
    # ========================================================================
    # DATASET LIST (now appears below indicators if exploring)
    # ========================================================================
    
    col1, col2, col3 = st.columns([3, 1, 1])
    
    with col1:
        dataset_search = st.text_input(
            "🔎 Search datasets",
            placeholder="e.g., education, poverty, climate",
            key="dataset_search"
        )
    
    with col2:
        sort_by = st.selectbox(
            "Sort by",
            ["Name (A-Z)", "Name (Z-A)", "Indicator Count (High-Low)", "Indicator Count (Low-High)", "Last Updated"],
            key="dataset_sort"
        )
    
    with col3:
        items_per_page = st.selectbox(
            "Items per page",
            [10, 20, 50, 100],
            index=1,
            key="items_per_page"
        )
    
    filtered_datasets = {}
    
    for db_id, info in DATABASE_CATALOG.items():
        if st.session_state.get('selected_organizations') and info['organization'] not in st.session_state.selected_organizations:
            continue
        
        if st.session_state.selected_themes:
            if not any(theme in info.get('themes', []) for theme in st.session_state.selected_themes):
                continue
        
        if dataset_search:
            search_lower = dataset_search.lower()
            if not (
                search_lower in info['name'].lower() or
                search_lower in info.get('description', '').lower() or
                search_lower in db_id.lower()
            ):
                continue
        
        filtered_datasets[db_id] = info
    
    if sort_by == "Name (A-Z)":
        sorted_datasets = sorted(filtered_datasets.items(), key=lambda x: x[1]['name'])
    elif sort_by == "Name (Z-A)":
        sorted_datasets = sorted(filtered_datasets.items(), key=lambda x: x[1]['name'], reverse=True)
    elif sort_by == "Indicator Count (High-Low)":
        sorted_datasets = sorted(filtered_datasets.items(), key=lambda x: x[1].get('indicator_count', 0), reverse=True)
    elif sort_by == "Indicator Count (Low-High)":
        sorted_datasets = sorted(filtered_datasets.items(), key=lambda x: x[1].get('indicator_count', 0))
    else:
        sorted_datasets = sorted(filtered_datasets.items(), key=lambda x: x[1]['name'])
    
    total_count = len(filtered_datasets)
    st.markdown(f"### Showing **1-{min(items_per_page, total_count)}** of **{total_count}** datasets")
    
    if st.session_state.selected_themes or st.session_state.get('selected_organizations'):
        st.markdown("**Active filters:**")
        filter_html = ""
        for theme in st.session_state.selected_themes:
            filter_html += f'<span class="tag">{theme}</span>'
        for org in st.session_state.get('selected_organizations', []):
            filter_html += f'<span class="database-badge">{org}</span>'
        st.markdown(filter_html, unsafe_allow_html=True)
    
    st.markdown("---")
    
    for idx, (db_id, info) in enumerate(sorted_datasets[:items_per_page], 1):
        with st.container():
            col1, col2 = st.columns([5, 1])
            
            with col1:
                st.markdown(f"### 🗂️ {info['name']}")
                
                st.caption(f"**Last updated:** Recently | **{info.get('indicator_count', 'N/A')}** Indicators")
                
                org_html = f'<span class="database-badge">{info["organization"]}</span>'
                st.markdown(org_html, unsafe_allow_html=True)
                
                st.markdown(f"*{info['description']}*")
                
                if info.get('themes'):
                    themes_html = " ".join([f'<span class="tag">{t}</span>' for t in info['themes'][:5]])
                    st.markdown(themes_html, unsafe_allow_html=True)
                
                st.caption(f"📊 Dataset ID: `{db_id}`")
            
            with col2:
                st.markdown(f"**{info.get('indicator_count', 'N/A')}**")
                st.caption("INDICATORS")
                
                if st.button("📈 Explore", key=f"dataset_explore_{db_id}", use_container_width=True):
                    st.session_state.exploring_database = db_id
                    st.session_state.exploring_db_name = info['name']
                    # Rerun to show indicators at top
                    st.rerun()
            
            st.markdown("---")
    
    if total_count > items_per_page:
        st.info(f"Showing first {items_per_page} datasets. Use filters to narrow results or increase items per page.")


# ============================================================================
# TAB 2: EXPLORE INDICATORS
# ============================================================================

with tab2:
    st.markdown("## Explore Indicators")
    
    # Debug toggle
    show_debug = st.checkbox("🐛 Show debug info", value=False, key="debug_mode")
    
    col1, col2, col3 = st.columns([3, 1, 1])
    
    with col1:
        search_query = st.text_input(
            "🔎 Search indicators",
            placeholder="e.g., GDP, poverty rate, CO2 emissions, education",
            key="search_main"
        )
    
    with col2:
        sort_option = st.selectbox(
            "Sort by",
            ["Relevance", "Name A-Z", "Database"],
            key="sort_option"
        )
    
    with col3:
        if 'last_search_results' in st.session_state and st.session_state.last_search_results:
            if st.button("🗑️ Clear", use_container_width=True, key="clear_search_results"):
                del st.session_state.last_search_results
                st.rerun()
    
    if st.session_state.selected_themes or st.session_state.get('selected_organizations'):
        st.markdown("**Active filters:**")
        filter_html = ""
        for theme in st.session_state.selected_themes:
            filter_html += f'<span class="tag">{theme}</span>'
        for org in st.session_state.get('selected_organizations', []):
            filter_html += f'<span class="database-badge">{org}</span>'
        st.markdown(filter_html, unsafe_allow_html=True)
    
    if st.button("🚀 Search", type="primary", use_container_width=True):
        if search_query or st.session_state.selected_themes or st.session_state.get('selected_organizations'):
            with st.spinner("Searching indicators..."):
                if show_debug:
                    st.info(f"Search query: {search_query}")
                    st.info(f"Themes: {st.session_state.selected_themes}")
                    st.info(f"Organizations: {st.session_state.get('selected_organizations')}")
                
                indicators, total_count = search_indicators_filtered(
                    query=search_query if search_query else "GDP",
                    themes=st.session_state.selected_themes if st.session_state.selected_themes else None,
                    organizations=st.session_state.get('selected_organizations') if st.session_state.get('selected_organizations') else None,
                    databases=st.session_state.selected_databases if st.session_state.selected_databases else None,
                    limit=100
                )
                
                if show_debug:
                    st.info(f"Found {len(indicators)} indicators, total count: {total_count}")
                
                st.session_state.last_search_results = {'indicators': indicators, 'total_count': total_count}
                st.rerun()
        else:
            st.warning("⚠️ Please enter a search term or select at least one filter")
    
    if 'last_search_results' in st.session_state and st.session_state.last_search_results:
        indicators = st.session_state.last_search_results['indicators']
        total_count = st.session_state.last_search_results['total_count']
        
        if len(indicators) == 0:
            st.warning("⚠️ **No results found from API search**")
            st.info("""
            **The Data360 search API may have issues. Try these alternatives:**
            
            1. **🗂️ Browse Datasets Tab** - Works reliably! Click on any database, then "Explore" to see all its indicators
            2. **Try simpler search terms** - Use single words like "GDP", "population", "health"
            3. **Remove filters** - Clear themes/organizations and try again
            4. **Use Quick Search buttons** below
            """)
            
            # Show quick database shortcuts
            st.markdown("### 🚀 Quick Access to Popular Databases:")
            quick_col1, quick_col2, quick_col3 = st.columns(3)
            
            with quick_col1:
                if st.button("📊 World Development Indicators (WB_WDI)", key="quick_wdi", use_container_width=True):
                    st.session_state.exploring_database = "WB_WDI"
                    st.session_state.exploring_db_name = "World Development Indicators"
                    st.info("✅ Go to 'Browse Datasets' tab to see 1,508 indicators!")
            
            with quick_col2:
                if st.button("💰 World Economic Outlook (IMF_WEO)", key="quick_weo", use_container_width=True):
                    st.session_state.exploring_database = "IMF_WEO"
                    st.session_state.exploring_db_name = "World Economic Outlook"
                    st.info("✅ Go to 'Browse Datasets' tab to see indicators!")
            
            with quick_col3:
                if st.button("🏛️ Governance Indicators (WB_WGI)", key="quick_wgi", use_container_width=True):
                    st.session_state.exploring_database = "WB_WGI"
                    st.session_state.exploring_db_name = "Worldwide Governance Indicators"
                    st.info("✅ Go to 'Browse Datasets' tab to see indicators!")
        else:
            st.markdown(f"### Results: Showing **{len(indicators)}** of **{total_count}** indicators")
        
        if indicators:
            by_database = defaultdict(list)
            for ind in indicators:
                by_database[ind['database_id']].append(ind)
            
            for db_id, db_indicators in sorted(by_database.items()):
                db_info = DATABASE_CATALOG.get(db_id, {"name": db_id, "organization": "Unknown"})
                
                with st.expander(
                    f"📊 **{db_info['name']}** ({db_id}) - {len(db_indicators)} indicators",
                    expanded=True
                ):
                    for ind in db_indicators[:20]:
                        col1, col2 = st.columns([4, 1])
                        
                        with col1:
                            st.markdown(f"**{ind['name']}**")
                            if ind['description']:
                                st.caption(ind['description'][:200] + "..." if len(ind['description']) > 200 else ind['description'])
                            st.caption(f"🆔 `{ind['id']}`")
                            
                            if ind['topics']:
                                topics_html = " ".join([f'<span class="tag">{t}</span>' for t in ind['topics'][:5]])
                                st.markdown(topics_html, unsafe_allow_html=True)
                        
                        with col2:
                            if st.button("📈 Query", key=f"query_{ind['id']}", use_container_width=True):
                                st.session_state.selected_indicator = ind
                                st.session_state.query_database = ind['database_id']
                                st.session_state.show_query_tab = True
                                st.rerun()
                        
                        st.markdown("---")
        else:
            st.info("💡 **No indicators found.** Try:")
            st.markdown("""
            - Adjusting your search terms
            - Removing some filters
            - Browsing the **Browse Datasets** tab to explore available data
            """)
    else:
        st.info("👋 **Welcome to Indicator Explorer!**")
        st.markdown("""
        ### How to get started:
        
        1. **🔍 Enter a search term** above (e.g., "GDP", "education", "health")
        2. **📚 Apply filters** at the top of the page (Themes or Organizations)
        3. **🚀 Click Search** to find indicators
        
        ### 💡 Example searches to try:
        
        - `population` - Find population statistics
        - `GDP` - Economic growth indicators
        - `climate` or `emissions` - Environmental data
        - `education` or `literacy` - Education statistics
        - `health` or `mortality` - Health indicators
        
        ### Or try these shortcuts:
        """)
        
        quick_col1, quick_col2, quick_col3, quick_col4 = st.columns(4)
        
        with quick_col1:
            if st.button("📊 Search GDP", use_container_width=True, key="quick_gdp"):
                st.session_state.quick_search = "GDP"
                st.rerun()
        with quick_col2:
            if st.button("👥 Search Population", use_container_width=True, key="quick_pop"):
                st.session_state.quick_search = "population"
                st.rerun()
        with quick_col3:
            if st.button("🌡️ Search Climate", use_container_width=True, key="quick_climate"):
                st.session_state.quick_search = "climate emissions"
                st.rerun()
        with quick_col4:
            if st.button("🎓 Search Education", use_container_width=True, key="quick_edu"):
                st.session_state.quick_search = "education"
                st.rerun()
        
        if 'quick_search' in st.session_state:
            with st.spinner(f"Searching for {st.session_state.quick_search}..."):
                indicators, total_count = search_indicators_filtered(
                    query=st.session_state.quick_search,
                    themes=None,
                    organizations=None,
                    databases=None,
                    limit=100
                )
                st.session_state.last_search_results = {'indicators': indicators, 'total_count': total_count}
                del st.session_state.quick_search
                st.rerun()


# ============================================================================
# TAB 3: QUERY & VISUALIZE
# ============================================================================

with tab3:
    st.markdown("## Query & Visualize Data")
    
    # Auto-load indicators if coming from Browse/Explore with a selection
    if 'selected_indicator' in st.session_state:
        ind = st.session_state.selected_indicator
        current_db = ind['database_id']
        
        # Auto-load indicators if not already loaded for this database
        if 'available_indicators' not in st.session_state or st.session_state.get('query_database') != current_db:
            with st.spinner(f"Loading indicators from {DATABASE_CATALOG.get(current_db, {}).get('name', current_db)}..."):
                st.session_state.available_indicators = get_indicators_with_metadata(current_db)
                st.session_state.query_database = current_db
                st.session_state.last_selected_db = current_db
        
        # Show selection banner
        col1, col2 = st.columns([5, 1])
        with col1:
            st.success(f"✅ Pre-selected: **{ind['name']}**")
        with col2:
            if st.button("❌", key="clear_selection", help="Clear selection"):
                del st.session_state.selected_indicator
                st.rerun()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Get current database selection
        current_db = st.session_state.get('query_database', 'WB_WDI')
        
        selected_db = st.selectbox(
            "Database",
            st.session_state.databases,
            index=st.session_state.databases.index(current_db) if current_db in st.session_state.databases else 0,
            key="query_db_select",
            help="Select a database to query"
        )
        
        # Detect database change
        if 'last_selected_db' not in st.session_state:
            st.session_state.last_selected_db = selected_db
        
        if selected_db != st.session_state.last_selected_db:
            # Database changed - clear everything
            st.session_state.last_selected_db = selected_db
            st.session_state.query_database = selected_db
            if 'available_indicators' in st.session_state:
                del st.session_state.available_indicators
            if 'selected_indicator' in st.session_state:
                del st.session_state.selected_indicator
            st.info(f"🔄 Database changed to {DATABASE_CATALOG.get(selected_db, {}).get('name', selected_db)}. Click 'Load Indicators' to see available indicators.")
    
    with col2:
        if st.button("📋 Load Indicators", use_container_width=True, type="primary"):
            with st.spinner(f"Loading indicators from {DATABASE_CATALOG.get(selected_db, {}).get('name', selected_db)}..."):
                # Clear old selection when loading new indicators
                if 'selected_indicator' in st.session_state:
                    del st.session_state.selected_indicator
                
                st.session_state.available_indicators = get_indicators_with_metadata(selected_db)
                st.session_state.query_database = selected_db
                st.success(f"✅ Loaded {len(st.session_state.available_indicators)} indicators")
                time.sleep(0.5)
                st.rerun()  # Force refresh to update dropdown
    
    with col3:
        indicator_count = len(st.session_state.get('available_indicators', []))
        st.metric("Available", indicator_count if indicator_count > 0 else "Click Load")
    
    if 'available_indicators' in st.session_state and st.session_state.available_indicators:
        indicator_names = {ind['id']: ind['name'] for ind in st.session_state.available_indicators}
        
        # Determine default selection
        default_indicator = None
        default_index = 0
        
        if 'selected_indicator' in st.session_state:
            ind = st.session_state.selected_indicator
            # Only use pre-selected if it's from the same database and exists in loaded indicators
            if ind['database_id'] == selected_db and ind['id'] in indicator_names:
                default_indicator = ind['id']
                default_index = list(indicator_names.keys()).index(default_indicator)
        
        selected_indicator_id = st.selectbox(
            "Select Indicator",
            options=list(indicator_names.keys()),
            format_func=lambda x: f"{indicator_names[x]} ({x})",
            index=default_index,
            key="query_indicator_select",
            help="Choose an indicator to query"
        )
        
        # Update session state with current selection
        selected_ind_details = next((ind for ind in st.session_state.available_indicators if ind['id'] == selected_indicator_id), None)
    else:
        # No indicators loaded yet
        st.warning("⚠️ No indicators loaded. Click '📋 Load Indicators' button above to get started.")
        st.info("""
        **Quick start:**
        1. Select a database from the dropdown above
        2. Click the '📋 Load Indicators' button
        3. Choose an indicator from the list that appears
        4. Set your query parameters and click 'Fetch Data'
        
        **Or:** Go to the '🗂️ Browse Datasets' tab and click 'Explore' on any database to browse indicators visually.
        """)
        selected_ind_details = None
        if selected_ind_details:
            with st.expander("ℹ️ Indicator Details", expanded=False):
                st.markdown(f"**Name:** {selected_ind_details['name']}")
                st.markdown(f"**ID:** `{selected_ind_details['id']}`")
                if selected_ind_details['description']:
                    st.markdown(f"**Description:** {selected_ind_details['description']}")
                if selected_ind_details['topics']:
                    topics_html = " ".join([f'<span class="tag">{t}</span>' for t in selected_ind_details['topics']])
                    st.markdown(f"**Topics:** {topics_html}", unsafe_allow_html=True)
        
        st.markdown("---")
        
        st.markdown("### 🎯 Query Parameters")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            countries_input = st.text_input(
                "Countries (comma-separated)",
                value="USA,GBR,DEU,FRA,JPN",
                help="Use ISO 3-letter country codes",
                key="query_countries"
            )
        
        with col2:
            year_from = st.text_input("From Year", "2010", key="query_year_from")
        
        with col3:
            year_to = st.text_input("To Year", "2023", key="query_year_to")
        
        if st.button("🚀 Fetch Data", type="primary", use_container_width=True):
            countries_list = [c.strip().upper() for c in countries_input.split(",") if c.strip()]
            
            if countries_list:
                with st.spinner("Fetching data..."):
                    data = fetch_data_cached(
                        selected_db,
                        selected_indicator_id,
                        countries_list,
                        year_from,
                        year_to
                    )
                    
                    if data:
                        st.session_state.current_data = data
                        st.session_state.current_indicator_name = indicator_names[selected_indicator_id]
                        st.success(f"✅ Fetched {len(data)} records")
                    else:
                        st.warning("⚠️ No data found for the selected parameters")
    
    if st.session_state.current_data:
        st.markdown("---")
        st.markdown("## 📊 Data Visualization")
        
        df = pd.DataFrame(st.session_state.current_data)
        df['OBS_VALUE'] = pd.to_numeric(df['OBS_VALUE'], errors='coerce')
        df['TIME_PERIOD'] = df['TIME_PERIOD'].astype(str)
        df = df.dropna(subset=['OBS_VALUE'])
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📈 Total Records", f"{len(df):,}")
        with col2:
            st.metric("🌍 Countries", df['REF_AREA'].nunique())
        with col3:
            st.metric("📅 Time Range", f"{df['TIME_PERIOD'].min()}-{df['TIME_PERIOD'].max()}")
        with col4:
            st.metric("📊 Avg Value", f"{df['OBS_VALUE'].mean():.2f}")
        
        viz_tab1, viz_tab2, viz_tab3 = st.tabs(["📈 Time Series", "📊 Comparison", "📋 Data Table"])
        
        with viz_tab1:
            fig = create_time_series_plot(
                df,
                "Time Series Analysis",
                st.session_state.current_indicator_name
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with viz_tab2:
            available_years = sorted(df['TIME_PERIOD'].unique(), reverse=True)
            selected_year = st.selectbox("Select Year", available_years, key="comparison_year")
            
            fig = create_comparison_chart(df, selected_year, "Country Comparison")
            st.plotly_chart(fig, use_container_width=True)
        
        with viz_tab3:
            st.dataframe(df, use_container_width=True, height=500)
        
        st.markdown("---")
        csv = df.to_csv(index=False)
        st.download_button(
            "📥 Download Data (CSV)",
            csv,
            f"data360_{selected_db}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            "text/csv",
            use_container_width=True
        )


# ============================================================================
# TAB 4: DATABASE CATALOG
# ============================================================================

with tab4:
    st.markdown("## 🗂️ Database Catalog")
    st.markdown("Browse all available databases and their characteristics")
    
    display_databases = {}
    for db_id, info in DATABASE_CATALOG.items():
        if st.session_state.get('selected_organizations') and info['organization'] not in st.session_state.selected_organizations:
            continue
        
        if st.session_state.selected_themes:
            if not any(theme in info.get('themes', []) for theme in st.session_state.selected_themes):
                continue
        
        display_databases[db_id] = info
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Databases", len(st.session_state.databases))
    with col2:
        st.metric("Showing", len(display_databases))
    with col3:
        org_count = len(set(info.get('organization', 'Unknown') for info in display_databases.values()))
        st.metric("Organizations", org_count)
    with col4:
        st.metric("Themes", len(THEME_TAXONOMY))
    
    st.markdown("---")
    
    st.markdown("### ⭐ Featured Databases")
    
    sorted_dbs = sorted(display_databases.items(), key=lambda x: x[1].get('indicator_count', 0), reverse=True)
    
    for db_id, info in sorted_dbs:
        with st.expander(f"**{info['name']}** ({db_id})", expanded=False):
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.markdown(f"**Organization:** {info['organization']}")
                st.markdown(f"**Description:** {info['description']}")
                
                if info.get('themes'):
                    themes_html = " ".join([f'<span class="tag">{t}</span>' for t in info['themes']])
                    st.markdown(f"**Themes:** {themes_html}", unsafe_allow_html=True)
                
                if info.get('update_frequency'):
                    st.caption(f"📅 Update Frequency: {info['update_frequency']}")
                
                if info.get('coverage'):
                    st.caption(f"🌍 Coverage: {info['coverage']}")
            
            with col2:
                if info.get('indicator_count'):
                    st.metric("Indicators", info['indicator_count'])
                
                if st.button("Explore", key=f"catalog_explore_{db_id}", use_container_width=True):
                    st.session_state.exploring_database = db_id
                    st.session_state.exploring_db_name = info['name']
                    # Force switch to Browse Datasets tab by rerunning
                    st.success(f"✅ Loading {info['name']}...")
                    st.info("🔄 Switching to 'Browse Datasets' tab...")
                    time.sleep(0.5)
                    st.rerun()
    
    st.markdown("---")
    st.markdown("### 📚 All Databases")
    
    if st.session_state.get('selected_organizations') or st.session_state.selected_themes:
        st.info(f"Showing {len(display_databases)} databases matching your filters")
    
    cols = st.columns(5)
    for idx, db in enumerate(sorted(display_databases.keys())):
        with cols[idx % 5]:
            if st.button(db, key=f"catalog_db_{db}", use_container_width=True):
                st.info(f"Selected: {display_databases[db]['name']}")


# ============================================================================
# TAB 5: BATCH DOWNLOAD
# ============================================================================

with tab5:
    st.markdown("## 💾 Batch Data Download")
    st.markdown("Download data for multiple indicators and countries efficiently")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📊 Select Indicators")
        
        batch_db = st.selectbox("Database", st.session_state.databases, key="batch_db_select")
        
        if st.button("📋 Load Indicators", key="batch_load_indicators", use_container_width=True):
            with st.spinner("Loading indicators..."):
                st.session_state.batch_indicators_list = get_indicators_with_metadata(batch_db, limit=500)
                st.success(f"Loaded {len(st.session_state.batch_indicators_list)} indicators")
        
        if 'batch_indicators_list' in st.session_state:
            indicator_options = {ind['id']: ind['name'] for ind in st.session_state.batch_indicators_list}
            
            selected_batch_indicators = st.multiselect(
                "Select indicators (max 10)",
                options=list(indicator_options.keys()),
                format_func=lambda x: f"{indicator_options[x][:50]}... ({x})",
                max_selections=10,
                key="batch_indicators"
            )
        else:
            selected_batch_indicators = []
    
    with col2:
        st.markdown("### 🌍 Select Countries & Period")
        
        countries_batch = st.multiselect(
            "Select countries",
            options=list(COMMON_COUNTRIES.keys()),
            default=["USA", "GBR", "DEU", "FRA", "JPN"],
            format_func=lambda x: f"{COMMON_COUNTRIES[x]} ({x})",
            key="batch_countries_multi"
        )
        
        if not countries_batch:
            countries_batch = []
        
        year_range_batch = st.slider(
            "Year Range",
            min_value=1960,
            max_value=2023,
            value=(2010, 2023),
            key="batch_year_range"
        )
        
        if 'batch_indicators_list' in st.session_state and selected_batch_indicators and countries_batch:
            years = year_range_batch[1] - year_range_batch[0] + 1
            estimated = len(selected_batch_indicators) * len(countries_batch) * years
            st.info(f"📊 Estimated records: ~{estimated:,}")
    
    st.markdown("---")
    
    selected_batch_indicators = st.session_state.get('selected_batch_indicators', [])
    countries_batch = st.session_state.get('countries_batch', [])
    year_range_batch = st.session_state.get('year_range_batch', (2010, 2023))
    batch_db = st.session_state.get('batch_db', 'WB_WDI')
    
    has_indicators = 'batch_indicators_list' in st.session_state and selected_batch_indicators
    has_countries = countries_batch and len(countries_batch) > 0
    
    if st.button("🚀 Start Batch Download", type="primary", use_container_width=True):
        if not has_indicators:
            st.error("Please select at least one indicator")
        elif not has_countries:
            st.error("Please select at least one country")
        else:
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            all_batch_data = []
            total_queries = len(selected_batch_indicators) * len(countries_batch)
            completed = 0
            
            indicator_options_map = {ind['id']: ind['name'] for ind in st.session_state.batch_indicators_list}
            
            for indicator in selected_batch_indicators:
                indicator_name = indicator_options_map.get(indicator, indicator)[:30]
                
                for country in countries_batch:
                    status_text.text(f"⏳ Fetching: {indicator_name}... for {COMMON_COUNTRIES.get(country, country)}")
                    
                    try:
                        data = fetch_data_cached(
                            batch_db,
                            indicator,
                            [country],
                            str(year_range_batch[0]),
                            str(year_range_batch[1])
                        )
                        all_batch_data.extend(data)
                    except Exception as e:
                        st.warning(f"⚠️ Failed: {indicator} - {country}: {str(e)}")
                    
                    completed += 1
                    progress_bar.progress(completed / total_queries)
                    time.sleep(0.05)
            
            status_text.text("✅ Batch download complete!")
            
            if all_batch_data:
                df_batch = pd.DataFrame(all_batch_data)
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Records", f"{len(df_batch):,}")
                with col2:
                    st.metric("Unique Countries", df_batch['REF_AREA'].nunique())
                with col3:
                    st.metric("Indicators", df_batch['INDICATOR'].nunique())
                
                st.markdown("### 📋 Preview")
                st.dataframe(df_batch.head(100), use_container_width=True, height=400)
                
                csv_batch = df_batch.to_csv(index=False)
                st.download_button(
                    "📥 Download Complete Dataset (CSV)",
                    csv_batch,
                    f"batch_download_{batch_db}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    "text/csv",
                    use_container_width=True
                )
            else:
                st.error("No data was retrieved. Please check your selections.")


# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown(
    f"""
    <div style='text-align: center; color: #666; font-size: 12px; padding: 20px;'>
        <p>🌍 <b>Data360 Explorer</b> | Powered by World Bank Data360 API</p>
        <p>Databases: {len(st.session_state.databases)} | Active filters: {len(st.session_state.selected_themes) + len(st.session_state.selected_databases)}</p>
        <p style='margin-top: 10px; color: #888;'>Built with Streamlit • Dark Mode Optimized</p>
    </div>
    """,
    unsafe_allow_html=True
)
