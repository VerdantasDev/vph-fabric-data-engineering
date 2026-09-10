# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Teams Extraction
teams_extraction_dict = [{'team_name':'VDT-DE-TEAM','channel_name':'Tagged Data Sharing'},
                             {'team_name':'VDT-DE-TEAM','channel_name':'General'}]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SharePoint Exraction

# sites_to_scrape = ['VerdantasProjectsCopilot2024/Project documents/Proposals',
#                    'VerdantasProjectsCopilot2024/Project documents/Reports',
#                    'VerdantasProjectsCopilot2024/Project documents/RFPs',
#                    'VerdantasProjectsCopilot2024/Documents/Proof of Concept/Proposals and Projects',
#                    'VerdantasProjectsCopilot2024/Documents/Proof of Concept/Resumes',
#                    'VerdantasProjectsCopilot2024/Documents/Proof of Concept/RFP Request For Proposal',
#                    'MarketingCommunications/Resumes/REBRANDED Resumes',
#                    'SustainabilityCommunity/Documents',
#                    'DigitialTechnologies/Documents',
#                    'ArtificialIntelligenceGroup/Documents'
#                    ]

sites_to_scrape = ['ResourcesWorkspace/Resumes'
                   ]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Skills Extraction

selected_users = 'ALL'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
