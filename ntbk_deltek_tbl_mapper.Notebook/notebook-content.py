# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

mapper = [
    ['LD', ['Employee']],
    ['FW_CustomColumnValuesData', []],
    ['EmployeeCustomTabFields', ['Employee']],
    ['ProjectCustomTabFields', ['WBS1']],
    ['CFGMainData', ['Company']],
    ['CFGTimeAnalysis', []],
    ['CFGTimeAnalysisHeadingsData', []],
    ['EMAllCompany', ['Employee']],
    ['LedgerAR', ['WBS1']],
    ['CFGEmployeeTypeDescriptions', []],
    ['CFGClientTypeDescriptions', []],
    ['AR', ['WBS1']],
    ['PR', ['WBS1']],
    ['EMCompany', ['Employee']],
    ['V_UnbilledDetailData_BI', ['WBS1']],
    ['V_RevRecDetailData', ['WBS1']],
    ['ADPCode', []],
    ['Clendor', ['ClientID']],
    ['ClendorProjectAssoc', ['ClientID']],
    ['CLAddress', ['ClientID']],
    ['Clients_ClientFolderList', ['CustClientID']]
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(mapper)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
