INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)

SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_latname,
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'MARRIED'
		 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'SINGLE'
		 ELSE 'n/a'
	END cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
		 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
		 ELSE 'n/a'
	END cst_gndr,
	cst_create_date
FROM (
	SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL) AS t 
WHERE t.flag_last = 1
