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
        """Search Data360"""
        url = f"{self.BASE_URL}/data360/searchv2"
        if not query or query.strip() == "":
            query = "*"
        
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
            
        response = self.session.post(url, json=payload, timeout=self.timeout)
        response.raise_for_status()
        return response.json()
    
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
    """Cached database discovery"""
    client = Data360Client()
    result = client.search("*", top=1000)
    databases = set()
    
    if "value" in result:
        for item in result["value"]:
            if "series_description" in item:
                db_id = item["series_description"].get("database_id")
                if db_id:
                    databases.add(db_id)
    
    return sorted(list(databases))


@st.cache_data(ttl=3600)
def get_indicators_with_metadata(database_id: str, limit: int = 500):
    """Get indicators with their metadata"""
    client = Data360Client()
    
    # Search for all indicators in this database
    filter_query = f"series_description/database_id eq '{database_id}' and type eq 'indicator'"
    result = client.search("*", top=limit, filter_by=filter_query)
    
    indicators = []
    if "value" in result:
        for item in result["value"]:
            desc = item.get("series_description", {})
            indicators.append({
                "id": desc.get("idno"),
                "name": desc.get("name"),
                "description": desc.get("description", ""),
                "topics": desc.get("topics", []),
                "source": desc.get("source", {}),
                "database_id": desc.get("database_id")
            })
    
    return indicators


@st.cache_data(ttl=3600)
def search_indicators_filtered(query: str, themes: List[str] = None, 
                               databases: List[str] = None, 
                               organizations: List[str] = None,
                               limit: int = 100):
    """Search with filters including organizations"""
    client = Data360Client()
    
    filters = ["type eq 'indicator'"]
    
    if themes:
        theme_filter = " or ".join([f"series_description/topics/any(t: t/name eq '{theme}')" for theme in themes])
        filters.append(f"({theme_filter})")
    
    # Filter by organization - get databases for selected organizations
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
    
    filter_str = " and ".join(filters) if filters else None
    
    result = client.search(query, top=limit, filter_by=filter_str)
    
    indicators = []
    if "value" in result:
        for item in result["value"]:
            desc = item.get("series_description", {})
            indicators.append({
                "id": desc.get("idno"),
                "name": desc.get("name"),
                "description": desc.get("description", ""),
                "topics": [t.get("name", "") for t in desc.get("topics", [])],
                "database_id": desc.get("database_id"),
                "source": desc.get("source", {})
            })
    
    return indicators, result.get("@odata.count", len(indicators))


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


# ============================================================================
# HEADER
# ============================================================================

col1, col2, col3 = st.columns([1, 3, 1])
with col2:
    st.markdown("""
        <div style='text-align: center; padding: 20px 0;'>
            <h1 style='font-size: 3rem; margin-bottom: 0; background: linear-gradient(90deg, #4a9eff 0%, #667eea 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent;'>
                🌍 Data360 Explorer
            </h1>
            <p style='color: #888; font-size: 1.1rem; margin-top: 10px;'>
                Discover insights from 89 global databases • 10,000+ indicators
            </p>
        </div>
    """, unsafe_allow_html=True)


# ============================================================================
# SIDEBAR - ADVANCED FILTERS
# ============================================================================

with st.sidebar:
    st.markdown("## 🔍 Filters")
    
    # Load databases
    if st.session_state.databases is None:
        with st.spinner("Loading databases..."):
            st.session_state.databases = discover_databases()
    
    # Clear filters
    if st.button("🔄 Clear All Filters", use_container_width=True):
        st.session_state.selected_themes = []
        st.session_state.selected_databases = []
        st.session_state.selected_organizations = []
        st.rerun()
    
    st.markdown("---")
    
    # Count databases by theme
    theme_counts = defaultdict(int)
    for db_id, info in DATABASE_CATALOG.items():
        for theme in info.get("themes", []):
            theme_counts[theme] += 1
    
    # Count databases by organization
    org_counts = defaultdict(int)
    for db_id, info in DATABASE_CATALOG.items():
        org = info.get("organization", "Unknown")
        org_counts[org] += 1
    
    # Themes & Topics Filter
    with st.expander("📚 **Themes & Topics**", expanded=True):
        theme_search = st.text_input(
            "Search themes",
            placeholder="Search...",
            key="theme_search",
            label_visibility="collapsed"
        )
        
        st.markdown("##### Select Themes")
        
        # Filter themes based on search
        available_themes = sorted(theme_counts.keys())
        if theme_search:
            available_themes = [t for t in available_themes if theme_search.lower() in t.lower()]
        
        # Show themes with counts
        for theme in available_themes:
            count = theme_counts[theme]
            col1, col2 = st.columns([4, 1])
            with col1:
                if st.checkbox(theme, key=f"theme_{theme}"):
                    if theme not in st.session_state.selected_themes:
                        st.session_state.selected_themes.append(theme)
                elif theme in st.session_state.selected_themes:
                    st.session_state.selected_themes.remove(theme)
            with col2:
                st.caption(f"({count})")
    
    # Source Organizations Filter
    with st.expander("🏢 **Source Organizations**", expanded=True):
        org_search = st.text_input(
            "Search organizations",
            placeholder="Search...",
            key="org_search",
            label_visibility="collapsed"
        )
        
        st.markdown("##### Select Organizations")
        
        # Filter organizations based on search
        available_orgs = sorted(org_counts.keys())
        if org_search:
            available_orgs = [o for o in available_orgs if org_search.lower() in o.lower()]
        
        # Initialize selected_organizations if not exists
        if 'selected_organizations' not in st.session_state:
            st.session_state.selected_organizations = []
        
        # Show organizations with counts
        for org in available_orgs:
            count = org_counts[org]
            col1, col2 = st.columns([4, 1])
            with col1:
                if st.checkbox(org, key=f"org_{org}"):
                    if org not in st.session_state.selected_organizations:
                        st.session_state.selected_organizations.append(org)
                elif org in st.session_state.selected_organizations:
                    st.session_state.selected_organizations.remove(org)
            with col2:
                st.caption(f"({count})")
    
    # Countries Filter
    with st.expander("🌐 **Countries**", expanded=False):
        st.session_state.selected_countries = st.multiselect(
            "Select countries",
            options=list(COMMON_COUNTRIES.keys()),
            format_func=lambda x: f"{COMMON_COUNTRIES[x]} ({x})",
            key="country_filter"
        )
    
    # Time Period Filter
    with st.expander("📅 **Time Period**", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            st.session_state.year_from = st.number_input(
                "From",
                min_value=1960,
                max_value=2025,
                value=2010,
                key="year_from_filter"
            )
        with col2:
            st.session_state.year_to = st.number_input(
                "To",
                min_value=1960,
                max_value=2025,
                value=2023,
                key="year_to_filter"
            )
    
    st.markdown("---")
    
    # Stats
    st.markdown("### 📊 Quick Stats")
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Databases", len(st.session_state.databases))
    with col2:
        active_filters = (
            len(st.session_state.selected_themes) + 
            len(st.session_state.get('selected_organizations', []))
        )
        st.metric("Active Filters", active_filters)


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
    st.markdown("Explore available datasets with detailed information")
    
    # Search and sort
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
    
    # Filter datasets based on selections
    filtered_datasets = {}
    
    for db_id, info in DATABASE_CATALOG.items():
        # Filter by organization
        if st.session_state.get('selected_organizations') and info['organization'] not in st.session_state.selected_organizations:
            continue
        
        # Filter by theme
        if st.session_state.selected_themes:
            if not any(theme in info.get('themes', []) for theme in st.session_state.selected_themes):
                continue
        
        # Filter by search
        if dataset_search:
            search_lower = dataset_search.lower()
            if not (
                search_lower in info['name'].lower() or
                search_lower in info.get('description', '').lower() or
                search_lower in db_id.lower()
            ):
                continue
        
        filtered_datasets[db_id] = info
    
    # Sort datasets
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
    
    # Display results count
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
    
    # Display datasets
    for idx, (db_id, info) in enumerate(sorted_datasets[:items_per_page], 1):
        with st.container():
            # Header with icon and name
            col1, col2 = st.columns([5, 1])
            
            with col1:
                st.markdown(f"### 🗂️ [{info['name']}](javascript:void(0))")
                
                # Metadata
                st.caption(f"**Last updated:** Recently | **{info.get('indicator_count', 'N/A')}** Indicators")
                
                # Organization badge
                org_html = f'<span class="database-badge">{info["organization"]}</span>'
                st.markdown(org_html, unsafe_allow_html=True)
                
                # Description
                st.markdown(f"*{info['description']}*")
                
                # Themes/Topics
                if info.get('themes'):
                    themes_html = " ".join([f'<span class="tag">{t}</span>' for t in info['themes'][:5]])
                    st.markdown(themes_html, unsafe_allow_html=True)
                
                # Database ID
                st.caption(f"📊 Dataset ID: `{db_id}`")
            
            with col2:
                st.markdown(f"**{info.get('indicator_count', 'N/A')}**")
                st.caption("INDICATORS")
                
                if st.button("📈 Explore", key=f"explore_{db_id}", use_container_width=True):
                    st.session_state.selected_db_explore = db_id
                    st.session_state.active_tab = 2
                    st.rerun()
            
            st.markdown("---")
    
    # Pagination info
    if total_count > items_per_page:
        st.info(f"Showing first {items_per_page} datasets. Use filters to narrow results or increase items per page.")


# ============================================================================
# TAB 2: EXPLORE INDICATORS
# ============================================================================

with tab2:
    st.markdown("## Explore Indicators")
    
    col1, col2 = st.columns([3, 1])
    
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
    
    # Display active filters
    if st.session_state.selected_themes or st.session_state.get('selected_organizations'):
        st.markdown("**Active filters:**")
        filter_html = ""
        for theme in st.session_state.selected_themes:
            filter_html += f'<span class="tag">{theme}</span>'
        for org in st.session_state.get('selected_organizations', []):
            filter_html += f'<span class="database-badge">{org}</span>'
        st.markdown(filter_html, unsafe_allow_html=True)
    
    # Search button
    if st.button("🚀 Search", type="primary", use_container_width=True) or search_query:
        with st.spinner("Searching indicators..."):
            indicators, total_count = search_indicators_filtered(
                query=search_query if search_query else "*",
                themes=st.session_state.selected_themes if st.session_state.selected_themes else None,
                organizations=st.session_state.get('selected_organizations') if st.session_state.get('selected_organizations') else None,
                databases=st.session_state.selected_databases if st.session_state.selected_databases else None,
                limit=100
            )
            
            st.markdown(f"### Results: Showing **{len(indicators)}** of **{total_count}** indicators")
            
            if indicators:
                # Group by database
                by_database = defaultdict(list)
                for ind in indicators:
                    by_database[ind['database_id']].append(ind)
                
                # Display results
                for db_id, db_indicators in sorted(by_database.items()):
                    db_info = DATABASE_CATALOG.get(db_id, {"name": db_id, "organization": "Unknown"})
                    
                    with st.expander(
                        f"📊 **{db_info['name']}** ({db_id}) - {len(db_indicators)} indicators",
                        expanded=True
                    ):
                        for ind in db_indicators[:20]:  # Show first 20
                            col1, col2 = st.columns([4, 1])
                            
                            with col1:
                                st.markdown(f"**{ind['name']}**")
                                if ind['description']:
                                    st.caption(ind['description'][:200] + "..." if len(ind['description']) > 200 else ind['description'])
                                st.caption(f"🆔 `{ind['id']}`")
                                
                                # Topics
                                if ind['topics']:
                                    topics_html = " ".join([f'<span class="tag">{t}</span>' for t in ind['topics'][:5]])
                                    st.markdown(topics_html, unsafe_allow_html=True)
                            
                            with col2:
                                if st.button("📈 Query", key=f"query_{ind['id']}", use_container_width=True):
                                    st.session_state.selected_indicator = ind
                                    st.session_state.active_tab = 1
                                    st.rerun()
                            
                            st.markdown("---")
            else:
                st.info("No indicators found. Try adjusting your filters or search terms.")


# ============================================================================
# TAB 3: QUERY & VISUALIZE
# ============================================================================

with tab3:
    st.markdown("## Query & Visualize Data")
    
    # Pre-filled if coming from search
    default_indicator = None
    default_db = "WB_WDI"
    
    if 'selected_indicator' in st.session_state:
        ind = st.session_state.selected_indicator
        default_indicator = ind['id']
        default_db = ind['database_id']
        st.info(f"📊 Selected: **{ind['name']}**")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        selected_db = st.selectbox(
            "Database",
            st.session_state.databases,
            index=st.session_state.databases.index(default_db) if default_db in st.session_state.databases else 0,
            key="query_db"
        )
    
    with col2:
        if st.button("📋 Load Indicators", use_container_width=True):
            with st.spinner("Loading indicators..."):
                st.session_state.available_indicators = get_indicators_with_metadata(selected_db)
                st.success(f"Loaded {len(st.session_state.available_indicators)} indicators")
    
    with col3:
        st.metric("Available Indicators", 
                 len(st.session_state.get('available_indicators', [])) if 'available_indicators' in st.session_state else 0)
    
    # Indicator selection
    if 'available_indicators' in st.session_state and st.session_state.available_indicators:
        indicator_names = {ind['id']: ind['name'] for ind in st.session_state.available_indicators}
        
        selected_indicator_id = st.selectbox(
            "Select Indicator",
            options=list(indicator_names.keys()),
            format_func=lambda x: f"{indicator_names[x]} ({x})",
            index=list(indicator_names.keys()).index(default_indicator) if default_indicator in indicator_names else 0,
            key="query_indicator"
        )
        
        # Show indicator details
        selected_ind_details = next((ind for ind in st.session_state.available_indicators if ind['id'] == selected_indicator_id), None)
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
        
        # Query parameters
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
        
        # Fetch button
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
    
    # Visualization section
    if st.session_state.current_data:
        st.markdown("---")
        st.markdown("## 📊 Data Visualization")
        
        df = pd.DataFrame(st.session_state.current_data)
        df['OBS_VALUE'] = pd.to_numeric(df['OBS_VALUE'], errors='coerce')
        df['TIME_PERIOD'] = df['TIME_PERIOD'].astype(str)
        df = df.dropna(subset=['OBS_VALUE'])
        
        # Insights
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📈 Total Records", f"{len(df):,}")
        with col2:
            st.metric("🌍 Countries", df['REF_AREA'].nunique())
        with col3:
            st.metric("📅 Time Range", f"{df['TIME_PERIOD'].min()}-{df['TIME_PERIOD'].max()}")
        with col4:
            st.metric("📊 Avg Value", f"{df['OBS_VALUE'].mean():.2f}")
        
        # Visualization tabs
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
        
        # Download
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
    
    # Apply filters from sidebar
    display_databases = {}
    for db_id, info in DATABASE_CATALOG.items():
        # Filter by organization
        if st.session_state.get('selected_organizations') and info['organization'] not in st.session_state.selected_organizations:
            continue
        
        # Filter by theme
        if st.session_state.selected_themes:
            if not any(theme in info.get('themes', []) for theme in st.session_state.selected_themes):
                continue
        
        display_databases[db_id] = info
    
    # Summary metrics
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
    
    # Featured databases
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
                
                if st.button("Explore", key=f"explore_{db_id}", use_container_width=True):
                    st.session_state.selected_databases = [db_id]
                    st.rerun()
    
    # All databases list
    st.markdown("---")
    st.markdown("### 📚 All Databases")
    
    if st.session_state.get('selected_organizations') or st.session_state.selected_themes:
        st.info(f"Showing {len(display_databases)} databases matching your filters")
    
    # Create columns for database list
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
    
    with col2:
        st.markdown("### 🌍 Select Countries & Period")
        
        # Country selection
        countries_batch = st.multiselect(
            "Select countries",
            options=list(COMMON_COUNTRIES.keys()),
            default=["USA", "GBR", "DEU", "FRA", "JPN"],
            format_func=lambda x: f"{COMMON_COUNTRIES[x]} ({x})",
            key="batch_countries_multi"
        )
        
        # Year range
        year_range_batch = st.slider(
            "Year Range",
            min_value=1960,
            max_value=2023,
            value=(2010, 2023),
            key="batch_year_range"
        )
        
        # Estimated records
        if selected_batch_indicators and countries_batch:
            years = year_range_batch[1] - year_range_batch[0] + 1
            estimated = len(selected_batch_indicators) * len(countries_batch) * years
            st.info(f"📊 Estimated records: ~{estimated:,}")
    
    # Start batch download
    st.markdown("---")
    
    if st.button("🚀 Start Batch Download", type="primary", use_container_width=True):
        if not selected_batch_indicators:
            st.error("Please select at least one indicator")
        elif not countries_batch:
            st.error("Please select at least one country")
        else:
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            all_batch_data = []
            total_queries = len(selected_batch_indicators) * len(countries_batch)
            completed = 0
            
            for indicator in selected_batch_indicators:
                indicator_name = indicator_options.get(indicator, indicator)[:30]
                
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
                    time.sleep(0.05)  # Rate limiting
            
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
                
                # Download
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
        <p style='margin-top: 10px; color: #888;'>Built by @Gsnchez • Dark Mode Optimized</p>
    </div>
    """,
    unsafe_allow_html=True
)
