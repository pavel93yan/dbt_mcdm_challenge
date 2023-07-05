Steps for adding data from new ad platforms into MCDM:
1) Create new stage models in staging folder with proper data types and fields names.
2) Add new CTE (it should look like the rest of CTEs) that use new stage model as a data source in paid_ads_basic_performance_structure model.
3) Add additional union clause in paid_ads_basic_performance_structure model with the next clause after it "select {{ var_column_names }} from name_of_new_models.