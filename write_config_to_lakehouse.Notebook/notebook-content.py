# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "24113e54-6f3f-4157-8c30-a3c5e66de623",
# META       "default_lakehouse_name": "VPC_Dev_Fablh_data",
# META       "default_lakehouse_workspace_id": "297572de-b7d7-4285-a88e-1388e2598d4a"
# META     }
# META   }
# META }

# CELL ********************

code = '''
teams_extraction_dict = [{'team_name':'VDT-DE-TEAM','channel_name':'Tagged Data Sharing'},
                             {'team_name':'VDT-DE-TEAM','channel_name':'General'}]

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

sites_to_scrape = ['VerdantasProjectsCopilot2024/Project documents/VPC_OLT DEMO data'
                   ]
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

file_path = "Files/Configs/m365_config.py"
notebookutils.fs.put(file_path, code, True) # Set the last parameter as True to overwrite the file if it existed already

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
