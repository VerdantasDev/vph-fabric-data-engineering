[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared SalesRawTable = let
  Source = Excel.Workbook(Web.Contents("https://hullinc-my.sharepoint.com/personal/hkamunuri_verdantas_com/Documents/Apps/Microsoft Power Query/Uploaded Files/Dataflow_Practice_Sample_Data 1.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "SalesRawTable", Kind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Order_ID", type text}, {"Order_Date", type date}, {"Customer_ID", type text}, {"Product_ID", type text}, {"Region_Code", type text}, {"Channel", type text}, {"Quantity", Int64.Type}, {"Unit_Price_INR", Int64.Type}, {"Discount_Pct", type number}, {"Shipping_Fee_INR", Int64.Type}, {"Order_Status", type text}, {"Payment_Method", type text}, {"Sales_Rep", type text}, {"Promo_Code", type text}, {"Load_Timestamp", type datetime}, {"Notes", type text}})
in
  #"Changed column type";
shared CustomersTable = let
  Source = Excel.Workbook(Web.Contents("https://hullinc-my.sharepoint.com/personal/hkamunuri_verdantas_com/Documents/Apps/Microsoft Power Query/Uploaded Files/Dataflow_Practice_Sample_Data 1.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "CustomersTable", Kind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Customer_ID", type text}, {"Customer_Name", type text}, {"Segment", type text}, {"City", type text}, {"State", type text}, {"Region_Code", type text}, {"Signup_Date", type date}, {"Email", type text}, {"Phone", Int64.Type}, {"Loyalty_Tier", type text}})
in
  #"Changed column type";
shared ProductsTable = let
  Source = Excel.Workbook(Web.Contents("https://hullinc-my.sharepoint.com/personal/hkamunuri_verdantas_com/Documents/Apps/Microsoft Power Query/Uploaded Files/Dataflow_Practice_Sample_Data 1.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "ProductsTable", Kind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Product_ID", type text}, {"Product_Name", type text}, {"Category", type text}, {"Subcategory", type text}, {"List_Price_INR", Int64.Type}, {"GST_Rate", type number}})
in
  #"Changed column type";
shared RegionsTable = let
  Source = Excel.Workbook(Web.Contents("https://hullinc-my.sharepoint.com/personal/hkamunuri_verdantas_com/Documents/Apps/Microsoft Power Query/Uploaded Files/Dataflow_Practice_Sample_Data 1.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "RegionsTable", Kind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Region_Code", type text}, {"Region_Name", type text}, {"Hub_City", type text}, {"Regional_Manager", type text}})
in
  #"Changed column type";
shared TargetsTable = let
  Source = Excel.Workbook(Web.Contents("https://hullinc-my.sharepoint.com/personal/hkamunuri_verdantas_com/Documents/Apps/Microsoft Power Query/Uploaded Files/Dataflow_Practice_Sample_Data 1.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "TargetsTable", Kind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Month", type date}, {"Region_Code", type text}, {"Revenue_Target_INR", Int64.Type}, {"Orders_Target", Int64.Type}})
in
  #"Changed column type";
shared ReturnsTable = let
  Source = Excel.Workbook(Web.Contents("https://hullinc-my.sharepoint.com/personal/hkamunuri_verdantas_com/Documents/Apps/Microsoft Power Query/Uploaded Files/Dataflow_Practice_Sample_Data 1.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "ReturnsTable", Kind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Return_ID", type text}, {"Order_ID", type text}, {"Return_Date", type date}, {"Return_Reason", type text}, {"Refund_Amount_INR", type number}, {"Return_Approved", type text}})
in
  #"Changed column type";
