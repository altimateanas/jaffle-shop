{{ config(
    materialized='table',
    query_tag='cisco_demo'
) }}

/*
Informatica Job group name: EDW_CX_TELE_DNAC_BR
Target table name: MT_INSTALLED_PROD_TELMTRY_DNAC_NCP_DATA
*/
-- Modification Log
----------------------------------------
/******
-- ChangeLog ID		| 	Date			|	Modified by		|	CR No./Jira Number		|	Change Description
-- Change_log_1       	   OCT-5-2023          natthulu             CR1191                  	 Metric name - DV_SDA_WIRLSS_CLNTS_CNTD_FBRC_CNT(Metric deleted)
-- Change_log_3       	   OCT-12-2023         ankusahu             CR1324                  	 Metric name - DV_SDA_WIRD_CLNTS_CNTD_FBRC_CNT (Metric deleted)
-- Change_log_4       	   JUL-31-2024         siadhimo             CR2068                  	 LEAN ACTIVE RECORD JOIN CONDITION ADDED
-- Change_log_5            13/Mar/2025         gsunnapu         CR 2268 CADD-5099   	             TO Change logic of INVENTORY_9K_SWITCHES_CNT Metrics
-- Change_log_6            01/APR/2025         gsunnapu         CR CHG1858918
-- Change_log_7            23/JUN/2025         ratgupta         CR CHG1929284 - optimization
********/



WITH MAXDT  AS (SELECT MAX(TO_DATE(RECORDEDAT)) AS MAX_SNAPSHOT_DT FROM {{ source('edw_telmtry_br_db_br', 'mt_cp_telmtry_dnac') }})
    ,MINDT  AS (SELECT COALESCE(MAX(SNAPSHOT_DATE),'1900-01-01') AS MAX_SNAPSHOT_DT FROM {{ source('edw_telmtry_br_db_br', 'mt_installed_prod_telmtry_dnac') }})
    ,MTDNAC AS (SELECT MEMBERID AS MEMBER_ID
                      ,TO_DATE(RECORDEDAT) SNAPSHOT_DATE
				      ,OLDEST_COLLECTEDON_DTE
                      ,COLLECTEDON_DTE
				      ,CAST(LAST_TELEMETRY_COLLECTED AS DATE) TELEMETRY_LAST_COLLECTED_ON_DATE
                 FROM {{ source('edw_telmtry_br_db_br', 'mt_cp_telmtry_dnac') }}
				 WHERE TO_DATE(RECORDEDAT) > (SELECT MAX_SNAPSHOT_DT FROM MINDT )
		       ) --select * from MTDNAC;
,MT_CP_TELMTRY_THRDNG_MNGD_DEV as (select * from   {{ source('edw_telmtry_br_db_br', 'mt_cp_telmtry_thrdng_mngd_dev') }}
                            where RECORDTYPE = 'CST_DNAC'
                            AND CREATED_BY_DEVICE > (SELECT MAX_SNAPSHOT_DT FROM MINDT )
                                ) --Change_log_7

,NCP_DATA_SS AS (SELECT * FROM {{ source('edw_telmtry_etl_db_ss', 'dnac_ncpdata') }} )  --Change_log_5
,NCP_DATA AS  (SELECT A.MEMBERID AS MEMBER_ID
                     ,CONVERT_TIMEZONE('UTC','America/Los_Angeles', A.RECORDEDAT::VARCHAR::TIMESTAMP_NTZ)::DATE AS SNAPSHOT_DATE
					 ,A.DATAQUERYNAME
					 ,A.DATAATTR1
					 ,A.DATAATTR2
					 ,A.DATAATTR3
					  -- ADDED BY TRANSFORM TEAM
					 ,A.DATAATTR4
					 ,A.DATAATTR5
                     ,CONVERT_TIMEZONE('UTC','America/Los_Angeles',TO_TIMESTAMP_NTZ(A.COLLECTEDON/1000))              AS COLLECTEDON_TS
					 ,ROW_NUMBER () OVER (PARTITION BY MEMBER_ID, SNAPSHOT_DATE, A.DATAQUERYNAME, A.DATAATTR1, A.DATAATTR2, A.DATAATTR3,
					   A.DATAATTR4,  A.DATAATTR5 ORDER BY A.COLLECTEDON DESC) AS RN     --Removing the duplicates , rANK FEASABLE
--					 ,ROW_NUMBER () OVER (PARTITION BY MEMBER_ID, A.DATAQUERYNAME ORDER BY A.SNAPSHOT_DATE DESC) AS RN
				FROM NCP_DATA_SS A  --Change_log_5
		        JOIN MTDNAC ON A.MEMBERID = MTDNAC.MEMBER_ID
                           AND CONVERT_TIMEZONE('UTC','America/Los_Angeles', A.RECORDEDAT::VARCHAR::TIMESTAMP_NTZ)::DATE = MTDNAC.SNAPSHOT_DATE
				WHERE ((A.DATAQUERYNAME IN ('host_type'
				                           ,'golden_tag_image_count'
										   ,'device_controllability_check'
										   ,'access_policy_scalable_groups'
										   ,'aca_group_based_policies'
										   ,'access_policy_access_contracts'
										   ,'fabric_ap_count'
										   ,'number_of_Onboarding_templates_created'
										   ,'network_profiles_associated_to_sites'
										   ,'sda_device_series'
										   ,'sda_virtual_networks'
                                           ,'fabric_ssid_count'
										   ,'sda_port_assignment_auth_mode'
                                           ,'ise_status_info'
										   ,'lic_device_type_info'
										   ,'lic_smart_agent_info'
										   ,'sda_connectivity_domains'
										  -- ,'network_profiles_count_by_namespace' --Change_log_4
										   ,'number_of_DayN_templates_provisioned_on_Devices_successfully'
										   ,'number_of_managed_devices_with_template_applied'
										     -- ADDED BY TRANSFORM TEAM START
										   ,'umbrella_enabled'
										   , 'addon_activation_passed_count_pid_device_family'
										   , 'site_member_details1'
										   , 'image_activation_passed_count_with_pid_serialnumber_device_family_goldenimage'
										   	 -- ADDED BY TRANSFORM TEAM	END
										   , 'inventory_platformtype' -- ADDED FOR TRANS VER2
										   , 'devices_successfully_replaced' -- ADDED FOR TRANS VER2
										   , 'site_member_details2'
										   ))
                       OR (A.DATAQUERYNAME LIKE 'sda_devices%' AND UPPER(A.DATAATTR2)  <> 'GLOBAL')
					   OR (A.DATAQUERYNAME = 'inventory_device_detail_2' AND UPPER(A.DATAATTR2) = 'MANAGED')
					   OR (A.DATAQUERYNAME = 'persistent_metrics' AND DATAATTR1='cnsr-reasoner.reasoner.count' AND DATAATTR3 IS NOT NULL AND DATAATTR3 <> '')
						OR ( A.DATAQUERYNAME = 'network_profiles_count_by_namespace' AND DATAATTR1 NOT IN ('dcp','switching_layer3','assurance','authentication','templates','switching_layer2',''))--Change_log_4
						-- ADDED BY TRANSFORM TEAM
						 OR (DATAQUERYNAME ='lic_virtual_account_info' and DATAATTR5 <> '' AND DATAATTR5 <> 'NA')
                      )
				  QUALIFY RN = 1
				  ) --SELECT * FROM NCP_DATA;
, NCP AS (SELECT MEMBER_ID
                ,SNAPSHOT_DATE
                ,GOLDEN_TAG_IMAGE_COUNT
                ,DEVICE_CONTROLLABILITY_ENABLED_FLAG
                ,SCALABLE_GROUPS_COUNT
                ,ACCESS_GRP_POLICY_COUNT
                ,ACCESS_POLICY_CONTRACT_COUNT
				,SDA_ACCESS_POINT_COUNT
				,ONBOARDING_TEMPLATE_COUNT
				,NETWRK_PRFL_ASSCTDTO_SITE_COUNT
				,DV_TMPLTES_PROVSNED_APPL_DVC_CNT
				,DV_TMPLTES_PROVSNED_MNGD_DVC_CNT
          FROM (SELECT MEMBER_ID
				,SNAPSHOT_DATE
                ,DATAQUERYNAME
				,CASE WHEN DATAQUERYNAME = 'golden_tag_image_count'                               THEN DATAATTR1
				      WHEN DATAQUERYNAME = 'device_controllability_check'  THEN CASE WHEN DATAATTR1=1 THEN 'Y' ELSE 'N' END
				      WHEN DATAQUERYNAME = 'access_policy_scalable_groups' /*AND DATAATTR2='ACTIVE' */THEN DATAATTR1 --CR908
				      WHEN DATAQUERYNAME = 'aca_group_based_policies'                             THEN DATAATTR1
				      WHEN DATAQUERYNAME = 'access_policy_access_contracts'                       THEN DATAATTR1
					  WHEN DATAQUERYNAME = 'fabric_ap_count'                                      THEN DATAATTR1
                      WHEN DATAQUERYNAME = 'number_of_Onboarding_templates_created'               THEN DATAATTR1
                      WHEN DATAQUERYNAME = 'network_profiles_associated_to_sites'                 THEN DATAATTR1
					  WHEN DATAQUERYNAME = 'number_of_DayN_templates_provisioned_on_Devices_successfully' THEN DATAATTR1
					  WHEN DATAQUERYNAME = 'number_of_managed_devices_with_template_applied'       THEN DATAATTR1

                END AS DATAQUERY_VAL
		   FROM NCP_DATA
          WHERE ((DATAQUERYNAME IN ('golden_tag_image_count'
                                   ,'aca_group_based_policies'
                                   ,'access_policy_access_contracts'
								   ,'fabric_ap_count'
                                   ,'number_of_Onboarding_templates_created'
                                   ,'network_profiles_associated_to_sites'
								   ,'number_of_DayN_templates_provisioned_on_Devices_successfully'
								   ,'number_of_managed_devices_with_template_applied'
								    )
                OR (DATAQUERYNAME = 'access_policy_scalable_groups' /*AND DATAATTR2='ACTIVE'*/)--CR908
                OR (DATAQUERYNAME = 'device_controllability_check'  AND DATAATTR1=1)
                 )))
          PIVOT(MAX(DATAQUERY_VAL) FOR DATAQUERYNAME IN('golden_tag_image_count'
                                                       ,'device_controllability_check'
                                                       ,'access_policy_scalable_groups'
                                                       ,'aca_group_based_policies'
                                                       ,'access_policy_access_contracts'
                                                       ,'ise_status_info'
                                                       ,'fabric_ap_count'
                                                       ,'number_of_Onboarding_templates_created'
                                                       ,'network_profiles_associated_to_sites'
													   ,'number_of_DayN_templates_provisioned_on_Devices_successfully'
													   ,'number_of_managed_devices_with_template_applied'

													   )
                        ) AS P (MEMBER_ID,SNAPSHOT_DATE,GOLDEN_TAG_IMAGE_COUNT,DEVICE_CONTROLLABILITY_ENABLED_FLAG,SCALABLE_GROUPS_COUNT,ACCESS_GRP_POLICY_COUNT,ACCESS_POLICY_CONTRACT_COUNT,ISE_INTEGRATION_FLAG
						        ,SDA_ACCESS_POINT_COUNT,ONBOARDING_TEMPLATE_COUNT,NETWRK_PRFL_ASSCTDTO_SITE_COUNT,DV_TMPLTES_PROVSNED_APPL_DVC_CNT,DV_TMPLTES_PROVSNED_MNGD_DVC_CNT
                               )
		) --select count(1) from NCP ;
,NCP_AGG AS (SELECT MEMBER_ID
                   ,SNAPSHOT_DATE
                   ,SUM(CASE WHEN DATAQUERYNAME = 'sda_device_series'  THEN (CASE WHEN UPPER(DATAATTR2) like '%SWITCH%' THEN DATAATTR3 ELSE 0 END) END) AS SDA_SWITCH_COUNT
                   ,SUM(CASE WHEN DATAQUERYNAME = 'sda_device_series'  THEN (CASE WHEN UPPER(DATAATTR2) like '%ROUTER%' THEN DATAATTR3 ELSE 0 END) END) AS SDA_ROUTER_COUNT
                   ,SUM(CASE WHEN DATAQUERYNAME = 'sda_device_series'  THEN (CASE WHEN UPPER(DATAATTR2) like '%WIRELESS% %LAN% %CONTROLLER%' OR UPPER(DATAATTR2)like '%WIRELESS% %CONTROLLER%' THEN DATAATTR3 ELSE 0 END) END) AS SDA_WLC_COUNT
                   ,SUM(CASE WHEN DATAQUERYNAME = 'sda_port_assignment_auth_mode' AND DATAATTR2 = 'Open Authentication'  THEN DATAATTR3 END) AS PORTS_OPEN_AUTH
                   ,SUM(CASE WHEN DATAQUERYNAME = 'sda_port_assignment_auth_mode' AND DATAATTR2 = 'No Authentication'    THEN DATAATTR3 END) AS PORTS_NO_AUTH
                   ,SUM(CASE WHEN DATAQUERYNAME = 'sda_port_assignment_auth_mode' AND DATAATTR2 = 'Closed Authentication'THEN DATAATTR3 END) AS PORTS_CLOSED_AUTH
                   ,SUM(CASE WHEN DATAQUERYNAME = 'sda_port_assignment_auth_mode' AND DATAATTR2 = 'Easy Connect'         THEN DATAATTR3 END) AS PORTS_EASY_CONNECT
                   ,COUNT(DISTINCT CASE WHEN DATAQUERYNAME LIKE 'sda_devices%' AND UPPER(DATAATTR2)  <> 'GLOBAL' THEN DATAATTR2 END) AS NON_GLOBAL_SITE_COUNT
				   ,SUM(CASE WHEN DATAQUERYNAME = 'host_type' AND UPPER(DATAATTR2)='WIRED' THEN DATAATTR1 ELSE 0 END) AS TOTAL_WIRED_CLIENT_DEVICES
				   ,SUM(CASE WHEN DATAQUERYNAME = 'host_type' AND UPPER(DATAATTR2)='WIRELESS' THEN DATAATTR1 ELSE 0 END) AS TOTAL_WIRELESS_CLIENT_DEVICES
			       ,COUNT(CASE WHEN DATAQUERYNAME = 'lic_smart_agent_info' AND (DATAATTR2 IS NOT NULL AND TRIM(DATAATTR2) <> '' AND UPPER(DATAATTR2) <> 'GLOBAL') THEN MEMBER_ID END) AS NTWRK_DEV_ASGN_TO_SITE_CNT
				   --,COUNT(CASE WHEN DATAQUERYNAME = 'inventory_device_detail_2' AND UPPER(DATAATTR2)='MANAGED' THEN MEMBER_ID END) AS INV_DEVICE_DTL_CNT
				   ,SUM(CASE WHEN DATAQUERYNAME = 'sda_connectivity_domains' AND TRIM(DATAATTR2) = '1' THEN DATAATTR1 END) AS DV_SDA_FABRIC_LAN_SITE_DOMAIN_CNT
				   ,SUM(CASE WHEN DATAQUERYNAME = 'sda_connectivity_domains' AND TRIM(DATAATTR2) = '4' THEN DATAATTR1 END) AS DV_SDA_FABRIC_SITE_DOMAIN_CNT
				   ,SUM(CASE WHEN DATAQUERYNAME = 'sda_connectivity_domains' AND TRIM(DATAATTR2) = '5' THEN DATAATTR1 END) AS DV_SDA_TRANSIT_SITE_DOMAIN_CNT
				   ,SUM(CASE WHEN DATAQUERYNAME = 'persistent_metrics' AND DATAATTR1 = 'cnsr-reasoner.reasoner.count'  THEN NVL(DATAATTR3,0) END) AS MACHINE_REASNG_CNT
				   ,SUM(CASE WHEN DATAQUERYNAME = 'network_profiles_count_by_namespace' THEN NVL(DATAATTR2,0) END) AS NETWRK_PRFL_NAMESPACE_COUNT
				   ,SUM(CASE WHEN DATAQUERYNAME = 'persistent_metrics' AND DATAATTR1 = 'cnsr-reasoner.reasoner.count' AND (DATAATTR2 like 'DSigAnalyzerUber%' OR  DATAATTR2 like 'openVuln%')  THEN NVL(DATAATTR3,0) END) AS SECURITY_VULNERAB_SCAN
				   ,SUM(CASE WHEN DATAQUERYNAME = 'sda_port_assignment_auth_mode' AND DATAATTR2 = 'Low Impact'  THEN DATAATTR3 END) AS PORTS_LOW_IMPACT_CNT,
				      -- added by TRANSFORM TEAM START
				   LISTAGG (DISTINCT CASE WHEN DATAQUERYNAME ='lic_virtual_account_info' THEN  DATAATTR5 END)  AS VIRTUAL_ACCOUNT_LIST,
                    --COUNT(DISTINCT CASE WHEN DATAQUERYNAME = 'site_member_details1' AND DATAATTR4 ='Switches and Hubs' THEN DATAATTR2 END) AS SWITCHES_ASSIGNED_TO_SITE_CNT,
                    --COUNT(DISTINCT CASE WHEN DATAQUERYNAME = 'site_member_details1' AND DATAATTR4 ='Routers' THEN DATAATTR2 END) AS ROUTERS_ASSIGNED_TO_SITE_CNT,
                    --COUNT(DISTINCT CASE WHEN DATAQUERYNAME = 'site_member_details1' AND DATAATTR4 ='Wireless Controller' THEN DATAATTR2 END) AS WLC_ASSIGNED_TO_SITE_CNT,
                    --COUNT(DISTINCT CASE WHEN DATAQUERYNAME = 'site_member_details1' AND DATAATTR4 ='Unified AP' THEN DATAATTR2 END) AS AP_ASSIGNED_TO_SITE_CNT,
                    SUM(CASE WHEN DATAQUERYNAME = 'umbrella_enabled' THEN DATAATTR1 END) AS UMBRELLA_INTEGRATN_ENABLED_DEVICE_CNT, -- CHECK IF DISTINCT IS REQUIRED
					--COUNT(DISTINCT CASE WHEN DATAQUERYNAME = 'inventory_device_detail_2' AND DATAATTR4 ='Switches and Hubs' AND UPPER(DATAATTR2)= 'MANAGED' THEN DATAATTR1 END) AS INVENTORY_SWITCHES_CNT,
                    --COUNT(DISTINCT CASE WHEN DATAQUERYNAME = 'inventory_device_detail_2' AND DATAATTR4 ='Routers' AND UPPER(DATAATTR2) = 'MANAGED'  THEN DATAATTR1 END) AS INVENTORY_ROUTERS_CNT,
                    --COUNT(DISTINCT CASE WHEN DATAQUERYNAME = 'inventory_device_detail_2' AND DATAATTR4 ='Wireless Controller' AND UPPER(DATAATTR2) =  'MANAGED' THEN DATAATTR1 END) AS INVENTORY_WLC_CNT,
                    --COUNT(DISTINCT CASE WHEN DATAQUERYNAME = 'inventory_device_detail_2' AND DATAATTR4 ='Unified AP' AND UPPER(DATAATTR2) = 'MANAGED' THEN DATAATTR1 END) AS INVENTORY_ACCESS_POINT_CNT,
                    SUM(CASE WHEN DATAQUERYNAME='addon_activation_passed_count_pid_device_family' AND DATAATTR1='SMU_SW' THEN 1 ELSE 0 END) AS SMU_UPGRADES_COMPLETED_CNT,
					-- ADDED FOR TRANS VER2
					--SUM(CASE WHEN DATAQUERYNAME = 'inventory_platformtype' AND UPPER(DATAATTR2) LIKE 'CISCO CATALYST 9%SERIES --SWITCHES'
					--THEN DATAATTR1 END) AS INVENTORY_9K_SWITCHES_CNT, Change_log_5 Commented as logic change required
					COUNT(DISTINCT CASE WHEN DATAQUERYNAME = 'devices_successfully_replaced' THEN DATAATTR1 END ) AS RMA_WORKFLOWS_COMPLETED_CNT
			 FROM NCP_DATA
	         WHERE ((DATAQUERYNAME IN('sda_device_series'
			                        ,'sda_port_assignment_auth_mode'
								    ,'host_type'
									,'lic_smart_agent_info'
									,'sda_connectivity_domains'
                                    ,'network_profiles_count_by_namespace'
									-- ADDED BY TRANSFORM TEAM START
									,'umbrella_enabled'
									,'addon_activation_passed_count_pid_device_family'
									,'site_member_details1'
									,'image_activation_passed_count_with_pid_serialnumber_device_family_goldenimage'
									 -- ADDED BY TRANSFORM TEAM END
									, 'inventory_platformtype' -- ADDED FOR TRANS VER2
									, 'devices_successfully_replaced'
								   )
                     )
                    OR (DATAQUERYNAME LIKE 'sda_devices%' AND UPPER(DATAATTR2)  <> 'GLOBAL')
					OR (DATAQUERYNAME = 'inventory_device_detail_2' AND UPPER(DATAATTR2)='MANAGED')
					OR (DATAQUERYNAME = 'persistent_metrics' AND DATAATTR1='cnsr-reasoner.reasoner.count')
					 OR (DATAQUERYNAME ='lic_virtual_account_info' and DATAATTR5 <> '' AND DATAATTR5 <> 'NA') -- ADDED BY TRANSFORM TEAM
                    )
             GROUP BY 1,2
			 )--SELECT * FROM NCP_AGG;   --2263, 3021
			 --Change_log_4
,NCP_SITE_MEMBER AS (SELECT
                    MEMBER_ID
                   ,SNAPSHOT_DATE
                    ,COUNT(DISTINCT CASE WHEN DATAQUERYNAME = 'site_member_details1' AND DATAATTR4 ='Switches and Hubs' THEN DATAATTR2 END) AS SWITCHES_ASSIGNED_TO_SITE_CNT
                    ,COUNT(DISTINCT CASE WHEN DATAQUERYNAME = 'site_member_details1' AND DATAATTR4 ='Routers' THEN DATAATTR2 END) AS ROUTERS_ASSIGNED_TO_SITE_CNT
                    ,COUNT(DISTINCT CASE WHEN DATAQUERYNAME = 'site_member_details1' AND DATAATTR4 ='Wireless Controller' THEN DATAATTR2 END) AS WLC_ASSIGNED_TO_SITE_CNT
                    ,COUNT(DISTINCT CASE WHEN DATAQUERYNAME = 'site_member_details1' AND DATAATTR4 ='Unified AP' THEN DATAATTR2 END) AS AP_ASSIGNED_TO_SITE_CNT

FROM NCP_DATA
INNER JOIN
(SELECT PRIMARY_KEY,SERIAL_NUMBER,CREATED_BY_DEVICE,INVENTORYDEVICEID_DEVICEID
FROM   MT_CP_TELMTRY_THRDNG_MNGD_DEV WHERE RECORDTYPE = 'CST_DNAC')  MT_CP
ON MT_CP.PRIMARY_KEY =  NCP_DATA.MEMBER_ID AND TO_DATE(MT_CP.CREATED_BY_DEVICE) = NCP_DATA.SNAPSHOT_DATE
AND  INVENTORYDEVICEID_DEVICEID=dataattr2
INNER JOIN (SELECT DISTINCT TELEMETRY_ID,
									  END_CUSTOMER_PARTY_KEY,
									  RULE_NAME ,
									  SRC_IDENTIFIER
								 FROM {{ source('edw_telmtry_br_db_br', 'n_installed_prod_telemetry') }} LEAN
								 --INNER JOIN MTDNAC ON MTDNAC.MEMBER_ID = LEAN.TELEMETRY_ID
								 WHERE TEL_RECORD_TYPE = 'CST_DNAC'
									 AND THREADING_CD = 'DIRECT_CR'
									 AND RULE_NAME = 'MDSN'
									 AND ACTIVE_FLAG='Y'
									 AND SOURCE_DELETED_FLG ='N'
							   ) LEAN ON MT_CP.PRIMARY_KEY = LEAN.TELEMETRY_ID
  AND MT_CP.SERIAL_NUMBER = LEAN.SRC_IDENTIFIER
GROUP BY 1,2)
,NCP_NON AS (SELECT MEMBER_ID
                   ,SNAPSHOT_DATE
                   ,COUNT(DISTINCT CASE WHEN DATAQUERYNAME LIKE 'sda_devices%' AND UPPER(DATAATTR2)  <> 'GLOBAL' THEN DATAATTR2 END) AS NON_GLOBAL_SITE_COUNT
			  FROM NCP_DATA
	         WHERE DATAQUERYNAME LIKE 'sda_devices%' AND UPPER(DATAATTR2)  <> 'GLOBAL'
             GROUP BY 1,2
			 )--SELECT * FROM NCP_AGG;   --2263, 3021
,NCP_SP AS (SELECT MEMBER_ID                                         ---Since fabric_ap_count is duplicated, seperated into another block here
                  ,SNAPSHOT_DATE
                  ,SDA_AP_COLLECTED_ON_DT
          FROM (SELECT MEMBER_ID
				,SNAPSHOT_DATE
                ,DATAQUERYNAME
				,CASE WHEN DATAQUERYNAME = 'fabric_ap_count' THEN TO_DATE(COLLECTEDON_TS) END AS DATAQUERY_VAL
		   FROM NCP_DATA
          WHERE DATAQUERYNAME = 'fabric_ap_count'
               )
          PIVOT(MAX(DATAQUERY_VAL) FOR DATAQUERYNAME IN('fabric_ap_count')
                                 ) AS P (MEMBER_ID
                                        ,SNAPSHOT_DATE
                                        ,SDA_AP_COLLECTED_ON_DT
                                        )
           ) --SELECT COUNT(1) FROM NCP_SP;
,NCP_ISE AS (
			  SELECT MEMBER_ID
					,SNAPSHOT_DATE
					,CASE WHEN (ISE_PRIMARY = 'TRUSTED' OR ISE_SECONDARY = 'TRUSTED') AND ISE_PXGRID = 'ACTIVE' THEN 'Y' ELSE 'N' END AS ISE_INTEGRATION_FLAG
			   FROM (
					 SELECT MEMBER_ID
						   ,SNAPSHOT_DATE
						   ,ISE_PRIMARY
						   ,ISE_SECONDARY
						   ,ISE_PXGRID
					  FROM (
							SELECT MEMBER_ID
								  ,SNAPSHOT_DATE
								  ,DATAATTR1
								  ,DATAATTR2
							 FROM NCP_DATA
							WHERE LOWER(DATAQUERYNAME) = 'ise_status_info'
							  AND UPPER(DATAATTR2) IN ('TRUSTED','ACTIVE')
						   )PIVOT(MIN(DATAATTR2) FOR DATAATTR1 in ('PRIMARY','SECONDARY','PXGRID'))
												   AS P (MEMBER_ID,SNAPSHOT_DATE,ISE_PRIMARY,ISE_SECONDARY,ISE_PXGRID)
							)
            )--SELECT COUNT(1) FROM NCP_ISE;
,NCP_LIC AS    ( SELECT MEMBER_ID
                   ,SNAPSHOT_DATE
	               ,SWITCHES_HUBS_COUNT
	               ,SITE_SWITCHES_HUBS
	               ,WIRELESS_CONTROLLER_COUNT
	               ,SITE_WIRELESS_CONTROLLER
	               ,COALESCE(SWITCHES_HUBS_COUNT,0)+COALESCE(WIRELESS_CONTROLLER_COUNT,0)+COALESCE(LIC_ROUTER_COUNT,0)+COALESCE(LIC_OTHER_DEVICE_COUNT,0) AS LIC_TOTAL_MANAGED_DEVICES_COUNT
	               ,COALESCE(SITE_SWITCHES_HUBS,0)+COALESCE(SITE_WIRELESS_CONTROLLER,0)+COALESCE(SITE_ROUTER_COUNT,0)+COALESCE(SITE_OTHER_DEVICE_COUNT,0) AS SITE_TOTAL_MANAGED_DEVICES_COUNT
	               ,LIC_ROUTER_COUNT
	               ,SITE_ROUTER_COUNT
	               ,LIC_OTHER_DEVICE_COUNT
	               ,SITE_OTHER_DEVICE_COUNT
			 FROM
				(SELECT X.MEMBER_ID
					   ,X.SNAPSHOT_DATE
					   ,COUNT(CASE WHEN UPPER(TRIM(X.PRODUCT_FAMILY)) IN('SWITCHES AND HUBS','SWITCH') THEN 'SWITCHES AND HUBS' END) AS SWITCHES_HUBS_COUNT
					   ,COUNT(CASE WHEN UPPER(TRIM(X.PRODUCT_FAMILY)) IN('SWITCHES AND HUBS','SWITCH') AND UPPER(TRIM(Y.SITE))<> 'UNASSIGNED' AND Y.SITE IS NOT NULL THEN 'SWITCHES AND HUBS' END) AS SITE_SWITCHES_HUBS
					   ,COUNT(CASE WHEN UPPER(TRIM(X.PRODUCT_FAMILY)) = 'WIRELESS CONTROLLER' THEN 'WIRELESS CONTROLLER' END) AS WIRELESS_CONTROLLER_COUNT
					   ,COUNT(CASE WHEN UPPER(TRIM(X.PRODUCT_FAMILY)) = 'WIRELESS CONTROLLER' AND UPPER(TRIM(Y.SITE))<> 'UNASSIGNED' AND Y.SITE IS NOT NULL THEN 'WIRELESS CONTROLLER' END) AS SITE_WIRELESS_CONTROLLER
					   ,COUNT(CASE WHEN UPPER(TRIM(X.PRODUCT_FAMILY)) = 'ROUTERS' THEN 'ROUTERS' END) AS LIC_ROUTER_COUNT
					   ,COUNT(CASE WHEN UPPER(TRIM(X.PRODUCT_FAMILY)) = 'ROUTERS' AND UPPER(TRIM(Y.SITE))<> 'UNASSIGNED' AND Y.SITE IS NOT NULL THEN 'ROUTERS' END) AS SITE_ROUTER_COUNT
					   ,COUNT(CASE WHEN UPPER(TRIM(X.PRODUCT_FAMILY)) NOT IN ('ROUTERS' , 'SWITCHES AND HUBS','SWITCH','WIRELESS CONTROLLER') THEN 'OTHER_DEVICE' END) AS LIC_OTHER_DEVICE_COUNT
					   ,COUNT(CASE WHEN UPPER(TRIM(X.PRODUCT_FAMILY)) NOT IN ('ROUTERS' , 'SWITCHES AND HUBS','SWITCH','WIRELESS CONTROLLER') AND UPPER(TRIM(Y.SITE))<> 'UNASSIGNED' AND Y.SITE IS NOT NULL THEN 'OTHER_DEVICE' END) AS SITE_OTHER_DEVICE_COUNT
				  FROM (
						  SELECT MEMBER_ID
								,SNAPSHOT_DATE
								,DATAQUERYNAME
								,DATAATTR1 AS DEVICE_ENTITY_ID
								,DATAATTR3 AS PRODUCT_FAMILY
				   FROM NCP_DATA
				   WHERE DATAQUERYNAME = 'lic_device_type_info'
						 ) X
			   LEFT JOIN (SELECT MEMBER_ID
								,SNAPSHOT_DATE
								,DATAQUERYNAME
								,DATAATTR1 AS DEVICE_ENTITY_ID
								,DATAATTR2 AS SITE
				   FROM NCP_DATA
				   WHERE DATAQUERYNAME = 'lic_smart_agent_info'
					 AND DATAATTR1 IS NOT NULL
						) Y ON X.MEMBER_ID        = Y.MEMBER_ID
						   AND X.SNAPSHOT_DATE    = Y.SNAPSHOT_DATE
						   AND X.DEVICE_ENTITY_ID = Y.DEVICE_ENTITY_ID
					GROUP BY 1,2
				)
			 )--SELECT COUNT(1) from NCP_LIC;

 --Change_log_5 Created the CTE INVENTORY_SERIAL_NUMBER_DATA from the CTE NCP_INVENTORY_DEVICE_DTL_CNT logic, as it is common logic  start
,INVENTORY_SERIAL_NBR_DATA AS (SELECT * FROM
									(	SELECT NCPDATA_INV.*,TRIM(SERIAL_NUMBER.VALUE::STRING) AS SERIAL_NUMBER
									FROM (SELECT A.MEMBERID AS MEMBER_ID
												 ,CONVERT_TIMEZONE('UTC','America/Los_Angeles', A.RECORDEDAT::VARCHAR::TIMESTAMP_NTZ)::DATE AS SNAPSHOT_DATE
												 ,A.DATAQUERYNAME
												 ,A.DATAATTR1
												 ,A.DATAATTR2
												 ,A.DATAATTR3
												 ,A.DATAATTR4
												 ,A.DATAATTR5
												 ,ROW_NUMBER () OVER (PARTITION BY MEMBER_ID, SNAPSHOT_DATE,
												 A.DATAQUERYNAME, A.DATAATTR1, A.DATAATTR2, A.DATAATTR3,
												 A.DATAATTR4,  A.DATAATTR5 ORDER BY A.COLLECTEDON DESC) AS RN
								         FROM NCP_DATA_SS  A
										 JOIN MTDNAC ON A.MEMBERID = MTDNAC.MEMBER_ID
										 AND CONVERT_TIMEZONE('UTC','America/Los_Angeles', A.RECORDEDAT::VARCHAR::TIMESTAMP_NTZ)::DATE =
                                          MTDNAC.SNAPSHOT_DATE
										 WHERE DATAQUERYNAME = 'inventory_device_detail_2'
										 QUALIFY RN = 1) NCPDATA_INV,
										LATERAL FLATTEN(INPUT=>SPLIT(DATAATTR5,','))  SERIAL_NUMBER
										 ) NCP
										INNER JOIN (SELECT DISTINCT TELEMETRY_ID,
														  END_CUSTOMER_PARTY_KEY,
														  RULE_NAME ,
														  SRC_IDENTIFIER
													FROM {{ source('edw_telmtry_br_db_br', 'n_installed_prod_telemetry') }} LEAN
													INNER JOIN MTDNAC ON MTDNAC.MEMBER_ID = LEAN.TELEMETRY_ID
													WHERE TEL_RECORD_TYPE = 'CST_DNAC'
													AND THREADING_CD = 'DIRECT_CR'
													AND RULE_NAME = 'MDSN'
													AND ACTIVE_FLAG='Y'
													AND SOURCE_DELETED_FLG ='N'
													) LEAN
										ON NCP.MEMBER_ID = LEAN.TELEMETRY_ID AND NCP.SERIAL_NUMBER = LEAN.SRC_IDENTIFIER )
, INVENTORY_SERIAL_NBR_CAT9K_CNT AS (
									  SELECT  MEMBER_ID
										,SNAPSHOT_DATE
										,COUNT(distinct INV_DATA.SERIAL_NUMBER)  AS INVENTORY_9K_SWITCHES_CNT
										FROM INVENTORY_SERIAL_NBR_DATA INV_DATA
										INNER JOIN (SELECT
                                                    PRIMARY_KEY,
                                                    SERIAL_NUMBER,
                                                    CREATED_BY_DEVICE,
													INVENTORYDEVICEID_DEVICEID,
                                                    TRIM(PID) AS INV_PID
												    FROM
                                                    MT_CP_TELMTRY_THRDNG_MNGD_DEV
                                                   	WHERE RECORDTYPE =  'CST_DNAC'
                                                    AND INV_PID LIKE ANY('C92%', 'C93%', 'C94%', 'C95%', 'C96%' )
													AND INV_PID IN (
																SELECT BK_PRODUCT_ID
																FROM
                                                                {{ source('edw_ref_br_db_br', 'r_products') }}
																WHERE BK_PRODUCT_TYPE_ID = 'SWITCH')
													GROUP BY 1,2,3,4,5
													) MT_CP

										ON MT_CP.PRIMARY_KEY =  INV_DATA.MEMBER_ID
										AND TO_DATE(MT_CP.CREATED_BY_DEVICE) = INV_DATA.SNAPSHOT_DATE
                                        AND  INVENTORYDEVICEID_DEVICEID=DATAATTR1
                                        GROUP BY 1, 2
									)
	-- Change_log_5 End
			 --Change_log_4
,NCP_INVENTORY_DEVICE_DTL_CNT as (SELECT MEMBER_ID
				,SNAPSHOT_DATE
                --,SERIAL_NUMBER
                ,COUNT( DATAATTR1) AS INV_DEVICE_DTL_CNT
                ,COUNT( CASE WHEN DATAQUERYNAME = 'inventory_device_detail_2' AND DATAATTR4 ='Switches and Hubs' AND UPPER(DATAATTR2)= 'MANAGED' THEN DATAATTR1 END) AS INVENTORY_SWITCHES_CNT
                ,COUNT( CASE WHEN DATAQUERYNAME = 'inventory_device_detail_2' AND DATAATTR4 ='Routers' AND UPPER(DATAATTR2) = 'MANAGED'  THEN DATAATTR1 END) AS INVENTORY_ROUTERS_CNT
                ,COUNT( CASE WHEN DATAQUERYNAME = 'inventory_device_detail_2' AND DATAATTR4 ='Wireless Controller' AND UPPER(DATAATTR2) =  'MANAGED' THEN DATAATTR1 END) AS INVENTORY_WLC_CNT
                ,COUNT( CASE WHEN DATAQUERYNAME = 'inventory_device_detail_2' AND DATAATTR4 ='Unified AP' AND UPPER(DATAATTR2) = 'MANAGED' THEN DATAATTR1 END) AS INVENTORY_ACCESS_POINT_CNT


				FROM  INVENTORY_SERIAL_NBR_DATA --Change_log_5
			    WHERE UPPER(DATAATTR2) = 'MANAGED'
				GROUP BY 1,2)
,ASSURANCE_DATA AS      (SELECT MEMBERID AS MEMBER_ID
                               ,CONVERT_TIMEZONE('UTC','America/Los_Angeles', RECORDEDAT::VARCHAR::TIMESTAMP_NTZ)::DATE AS SNAPSHOT_DATE
							   ,COUNT_NAME
							   ,COUNTS
							   ,ROW_NUMBER() OVER (PARTITION BY  MEMBER_ID, SNAPSHOT_DATE, COUNT_NAME ORDER BY COLLECTEDON DESC) AS ROW_NUM    ----CHECK FOR RANK
						  FROM (SELECT RECORDEDAT
                                      ,MEMBERID
						              ,COUNT_NAME
									  ,COUNT AS COUNTS
									  ,COLLECTEDON
							     FROM {{ source('edw_telmtry_etl_db_ss', 'dnac_assurancecounts') }} A
						         INNER JOIN MTDNAC ON A.MEMBERID =  MTDNAC.MEMBER_ID
                                                  AND CONVERT_TIMEZONE('UTC','America/Los_Angeles', A.RECORDEDAT::VARCHAR::TIMESTAMP_NTZ)::DATE = MTDNAC.SNAPSHOT_DATE
								 WHERE A.COUNT_NAME IN ('ap_count'
													   ,'ap_site_count'
													   ,'wireless_count_max'
													   ,'wired_count_max'
													   )
								GROUP BY 1,2,3,4,5
								)
							QUALIFY ROW_NUM = 1
				)

,ASSURANCE_AGG AS (SELECT MEMBER_ID
                     ,SNAPSHOT_DATE
                     ,SUM(CASE WHEN COUNT_NAME = 'ap_count'           THEN COUNTS END) AS AP_COUNT
                     ,SUM(CASE WHEN COUNT_NAME = 'ap_site_count'      THEN COUNTS END) AS AP_SITE_COUNT
                     ,SUM(CASE WHEN COUNT_NAME = 'wireless_count_max' THEN COUNTS END) AS ASSURANCE_MAX_WIRELESS_CLIENTS_CONNECTED
                     ,SUM(CASE WHEN COUNT_NAME = 'wired_count_max'    THEN COUNTS END) AS ASSURANCE_MAX_WIRED_CLIENTS_CONNECTED
                FROM ASSURANCE_DATA
                GROUP BY 1,2
     )
,MT_NCP AS (SELECT A.PRIMARYKEY AS MEMBER_ID
        	      ,TO_DATE(A.CREATED_BY_DEVICE) AS SNAPSHOT_DATE
        	      ,SUM(CASE WHEN UPPER(TRIM(A.DATAQUERYNAME)) = 'SDA_VIRTUAL_NETWORKS'           THEN COUNT END) AS SDA_VN_COUNT
        	      ,SUM(CASE WHEN UPPER(TRIM(A.DATAQUERYNAME)) = 'FABRIC_SSID_COUNT'              THEN COUNT END) AS SDA_FABRIC_SSID_COUNT
        		  ,SUM(CASE WHEN A.DATAQUERYNAME = 'sda_devices' AND A.TYPE <> 'EndpointProxy'   THEN COUNT END) AS SDA_DEVICES_COUNT

				  ,SUM(CASE WHEN A.DATAQUERYNAME = 'sda_fabric_ip_pools' THEN COUNT ELSE 0 END) AS DV_SDA_FABRIC_IP_POOLS_COUNT
                  ,SUM(CASE WHEN A.DATAQUERYNAME = 'sda_fabric_connected_hosts' THEN COUNT ELSE 0 END) AS DV_SDA_CONCTD_TO_FBRC_CLNTS_CNT
                  ,SUM(CASE WHEN A.DATAQUERYNAME = 'sda_devices' AND TRIM(UPPER(REGEXP_REPLACE(TYPE,' ',''))) LIKE '%EDGENODE%' THEN COUNT ELSE 0 END) AS DV_SDA_EDGE_NODE_DEVICE_CNT
                  ,SUM(CASE WHEN A.DATAQUERYNAME = 'sda_devices' AND TRIM(UPPER(REGEXP_REPLACE(TYPE,' ',''))) LIKE '%BORDERNODE%' THEN COUNT ELSE 0 END) AS DV_SDA_BORDER_NODE_DEVICE_CNT
                  ,SUM(CASE WHEN A.DATAQUERYNAME = 'sda_devices' AND TRIM(UPPER(REGEXP_REPLACE(TYPE,' ',''))) LIKE '%MAPSERVER%' THEN COUNT ELSE 0 END) AS DV_SDA_CNTRL_NODE_DEVICE_CNT

                  --,SUM(CASE WHEN A.DATAQUERYNAME = 'sda_fabric_connected_hosts'  AND trim(upper(regexp_replace(type,' ',''))) like '%NUMBEROFWIREDHOSTS%' THEN COUNT ELSE 0 END) AS DV_SDA_WIRD_CLNTS_CNTD_FBRC_CNT --Change_log_3
                  --,SUM(CASE WHEN A.DATAQUERYNAME = 'sda_fabric_connected_hosts'  AND trim(upper(regexp_replace(type,' ',''))) like '%NUMBEROFWIRELESSHOSTS%' THEN COUNT ELSE 0 END) AS DV_SDA_WIRLSS_CLNTS_CNTD_FBRC_CNT --Change_log_1

	 FROM {{ source('edw_telmtry_br_db_br', 'mt_cp_telmtry_dnac_ncp') }}  A
              JOIN MTDNAC ON A.PRIMARYKEY =  MTDNAC.MEMBER_ID AND TO_DATE(A.CREATED_BY_DEVICE) = MTDNAC.SNAPSHOT_DATE
        	 WHERE A.RECORDTYPE = 'CST_DNAC'
			 GROUP BY 1,2
        	)
,FY_CALENDAR AS (
SELECT DISTINCT CONVERT_TIMEZONE('UTC','America/Los_Angeles', RECORDEDAT::VARCHAR::TIMESTAMP_NTZ)::DATE AS CALENDAR_DATE
FROM {{ source('edw_telmtry_etl_db_ss', 'dnac_base') }} WHERE CALENDAR_DATE <= (SELECT MAX_SNAPSHOT_DT FROM MAXDT ) ) -- <= MAXDT max_SNAPSHOST_DATE
    , CLICKEVENTAPISTAT_CTE AS (
	SELECT
		MEMBERID AS MEMBER_ID,
		FY_CALENDAR.CALENDAR_DATE AS SNAPSHOT_DATE,
		SUM( CASE WHEN EVENTURL = '/dna/assurance/dashboards/health/network' THEN 1 ELSE 0 END ) as NTWRK_HLTH_PAGE_VW_CUMLTV_CNT,
		SUM( CASE WHEN EVENTURL = '/dna/assurance/dashboards/health/client' THEN 1 ELSE 0 END ) as CLIENT_HLTH_PAGE_VW_CUMLTV_CNT,
		SUM( CASE WHEN EVENTURL like '/dna/assurance/dashboards/health/application%' THEN 1 ELSE 0 END ) as APPLCTN_HLTH_PAGE_VW_CUMLTV_CNT,
		SUM( CASE WHEN EVENTURL = '/dna/assurance/dashboards/health/networkServices/dhcp' THEN 1 ELSE 0 END ) as NSA_DHCP_PAGE_VW_CUMLTV_CNT,
		SUM( CASE WHEN EVENTURL = '/dna/assurance/dashboards/health/networkServices/aaa'THEN 1 ELSE 0 END ) as NSA_AAA_PAGE_VW_CUMLTV_CNT,
		SUM( CASE WHEN EVENTURL = '/dna/assurance/dashboards/health/sdaccess' THEN 1 ELSE 0 END ) as SDA_HEALTH_PAGE_VW_CUMLTV_CNT,
        SUM( CASE WHEN EVENTURL = '/dna/assurance/dashboards/poe' THEN 1 ELSE 0 END ) as POE_ANALYTICS_PAGE_VW_CUMLTV_CNT,
		SUM( CASE WHEN EVENTURL = '/dna/assurance/dashboards/issues/open' THEN 1 ELSE 0 END ) as ISSUES_DASHBOARD_PAGE_VW_CUMLTV_CNT,
		SUM( CASE WHEN EVENTURL like '%dna/assurance/client/details%view=airsense' THEN 1 ELSE 0 END ) as ICAP_CLIENT_PAGE_VW_CUMLTV_CNT,
		SUM( CASE WHEN EVENTURL like '%dna/assurance/device/details%view=airsense' THEN 1 ELSE 0 END ) as ICAP_AP_PAGE_VW_CUMLTV_CNT,
		SUM( CASE WHEN EVENTURL = '/dna/assurance/dashboards/health/overall' THEN 1 ELSE 0 END ) as OVERALL_HEALTH_VIEW_CUMLTV_CNT,
		SUM( CASE WHEN EVENTURL = '/dna/assurance/dashboards/wireless' THEN 1 ELSE 0 END) AS WIFI6_ANALYTICS_VIEW_CUMLTV_CNT,
		SUM( CASE WHEN EVENTURL= '/dna/assurance/dashboards/roguemgmtDashboard/threats' THEN 1 ELSE 0 END ) as ROGUE_DASHBOARD_VIEW_CUMLTV_CNT,
		SUM( CASE WHEN EVENTURL= '/dna/assurance/trends/trends-and-insights' THEN 1 ELSE 0 END ) as TREND_AND_INSIGHT_VIEW_CUMLTV_CNT,
		SUM( CASE WHEN EVENTURL= '/dna/assurance/trends/heatmap' THEN 1 ELSE 0 END ) as NETWORK_HEATMAP_VIEW_CUMLTV_CNT,
		SUM( CASE WHEN EVENTURL= '/dna/assurance/trends/baselines' THEN 1 ELSE 0 END ) as BASELINES_VIEW_CUMLTV_CNT,
		SUM( CASE WHEN EVENTURL = '/dna/tools/licenseManagement/dashboard/overview' THEN 1 ELSE 0 END ) as LICENSE_MANAGER_PAGE_VIEW_CUMLTV_CNT,
		SUM( CASE WHEN EVENTURL = '/dna/policy/dcs/welcome' THEN 1 ELSE 0 END ) as ENDPOINT_ANALYTICS_VIEW_CUMLTV_CNT,
		SUM( CASE WHEN EVENTURL = '/dna/assurance/dashboards/sensorDashboard' THEN 1 ELSE 0 END ) as SENSOR_DASHBOARD_VIEW_CUMLTV_CNT,
		SUM( CASE WHEN EVENTURL = '/dna/activity/auditLogs' THEN 1 ELSE 0 END ) as AUDIT_LOG_VIEW_CUMLTV_CNT,
		SUM( CASE WHEN EVENTURL LIKE '/data-sets-reports?reports-item=usage-insights' THEN 1 ELSE 0 END ) as USAGE_INSIGHT_VIEW_CUMLTV_CNT,
		SUM( CASE WHEN EVENTURL = '/dna/design/swim' THEN 1 ELSE 0 END ) as IMAGE_REPOSITORY_VIEW_CUMLTV_CNT,
		SUM( CASE WHEN EVENTURL = '/dna/manage/devices/list' THEN 1 ELSE 0 END ) as INVENTORY_PAGE_VIEW_CUMLTV_CNT,
		SUM( CASE WHEN EVENTURL = '/dna/tools/bug-identifier/overview' THEN 1 ELSE 0 END ) as NETWORK_BUG_IDENTIFIER_VIEW_CUMLTV_CNT
    FROM {{ source('edw_telmtry_etl_db_ss', 'dnac_clickeventapistat') }} CEAPI
	JOIN FY_CALENDAR
	ON FY_CALENDAR.CALENDAR_DATE >= COLLECTEDON::VARCHAR::TIMESTAMP_NTZ::DATE  AND FY_CALENDAR.CALENDAR_DATE <= (SELECT MAX_SNAPSHOT_DT FROM MAXDT )
    JOIN MTDNAC
    ON MTDNAC.MEMBER_ID = CEAPI.MEMBERID
    AND MTDNAC.SNAPSHOT_DATE = FY_CALENDAR.CALENDAR_DATE
    WHERE EVENTACTION = 'click'
	AND  COLLECTEDON::varchar::TIMESTAMP_NTZ::DATE = CONVERT_TIMEZONE('UTC','America/Los_Angeles', RECORDEDAT::VARCHAR::TIMESTAMP_NTZ):: date
	GROUP BY 1, 2
	)
,PRODUCT   AS (SELECT BK_PRODUCT_ID
                     ,DV_ITEM_TYPE_CD
                     ,BK_PRODUCT_TYPE_ID
                     ,PRODUCT_FAMILY_DESCRIPTION
                     ,PRODUCT_DESCRIPTION
                     ,RU_BK_PRODUCT_FAMILY_ID
                     ,BK_BUSINESS_UNIT_ID
                     ,CASE WHEN (UPPER(BK_PRODUCT_ID) RLIKE CONCAT('.*','AP','.*') ) THEN 'ACCESS POINT' --AND (UPPER(TRIM(DV_ITEM_TYPE_CD)) = 'CHASSIS')
                           WHEN (UPPER(PRODUCT_DESCRIPTION) RLIKE CONCAT('.*','WIRELESS CONTROLLER','.*') ) THEN 'WLC'  --AND (UPPER(TRIM(DV_ITEM_TYPE_CD)) = 'CHASSIS')
                           WHEN (UPPER(TRIM(BK_PRODUCT_TYPE_ID)) = 'ROUTER') THEN 'ROUTER'
                           WHEN (UPPER(TRIM(BK_PRODUCT_TYPE_ID)) = 'SWITCH') THEN 'SWITCH'
                        ELSE 'NA'END AS HARDWARE_PRODUCT_TYPE
                     FROM {{ source('edw_ref_br_db_br', 'r_products') }} A
)  --SELECT COUNT(1) FROM PRODUCT;
, NCP_SWIM_CTE AS (
                    SELECT
                        MEMBER_ID,
                        SNAPSHOT_DATE,
                        SUM (CASE WHEN DATAATTR3 = 'Switches and Hubs' THEN DEVICE_UPGRADES ELSE 0 END) AS SWIM_UPGRADED_SWITCH_CNT,
                        SUM (CASE WHEN DATAATTR3 = 'Wireless Controller' THEN DEVICE_UPGRADES ELSE 0 END) AS SWIM_UPGRADED_WLC_CNT,
                        SUM (CASE WHEN DATAATTR3 = 'Routers' THEN DEVICE_UPGRADES ELSE 0 END) AS SWIM_UPGRADED_ROUTERS_CNT
                    FROM(
                            SELECT
                                NCPDATA.MEMBERID AS MEMBER_ID,
                                NCPDATA.DATAATTR3,
                                NCPDATA.DATAATTR4,
                                FY_CALENDAR.CALENDAR_DATE AS SNAPSHOT_DATE,
                                COUNT(DISTINCT DATAATTR5) AS DEVICE_UPGRADES
                            FROM  NCP_DATA_SS NCPDATA  --Change_log_5
                           JOIN FY_CALENDAR
                            ON FY_CALENDAR.CALENDAR_DATE >= COLLECTEDON:: VARCHAR::TIMESTAMP_NTZ::DATE  and FY_CALENDAR.CALENDAR_DATE <= (SELECT MAX_SNAPSHOT_DT FROM MAXDT )
						   JOIN MTDNAC
							 ON MTDNAC.MEMBER_ID = NCPDATA.MEMBERID
							 AND MTDNAC.SNAPSHOT_DATE = FY_CALENDAR.CALENDAR_DATE
                            WHERE DATAQUERYNAME = 'image_activation_passed_count_with_pid_serialnumber_device_family_goldenimage'
                            AND DATAATTR3 IN ('Switches and Hubs', 'Routers', 'Wireless Controller')
                            GROUP BY 1, 2, 3, 4
                           )
                    GROUP BY 1, 2
                    )
-- ADDED BY TRANSFORM TEAM END
--added for CRs - 973,974,975,976,977
,IOTBU_INV_AGG AS (SELECT MEMBER_ID
                    ,SNAPSHOT_DATE
                    ,COALESCE(INVENTORY_IOT_SWITCHES_COUNT, 0) AS INVENTORY_IOT_SWITCHES_COUNT --1.	# of industrial switches in inventory
                    ,COALESCE(INVENTORY_IOT_ROUTERS_COUNT, 0)  AS INVENTORY_IOT_ROUTERS_COUNT  --2.	# of industrial routers in inventory
                    ,COALESCE(INVENTORY_IOT_ACCS_PNT_COUNT, 0) AS INVENTORY_IOT_ACCS_PNT_COUNT --3.	# of industrial APs in inventory
                     FROM
                    (
                      SELECT M.MEMBER_ID
                        ,M.SNAPSHOT_DATE
                        ,COALESCE(CX_SOL.LEVEL5_COMP_NAME,'NA') AS LEVEL5_COMP_NAME
                        ,COUNT(DISTINCT M.SERIAL_NUMBER ) AS SERIAL_NUMBER_CNT
                     FROM
                          (SELECT MNGD_DEV.PRIMARY_KEY AS MEMBER_ID
                                    ,TO_DATE(MNGD_DEV.CREATED_BY_DEVICE) AS SNAPSHOT_DATE
                                    ,MNGD_DEV.SERIAL_NUMBER
                                    ,TRIM(MNGD_DEV.PID) AS PID
                               FROM (SELECT MT_CP.PRIMARY_KEY
										   ,MT_CP.CREATED_BY_DEVICE
										   ,MT_CP.INVENTORYDEVICEID_DEVICEID
										   ,MT_CP.SERIAL_NUMBER
										   ,MT_CP.PID
									 FROM   MT_CP_TELMTRY_THRDNG_MNGD_DEV MT_CP
							         JOIN MTDNAC ON MT_CP.PRIMARY_KEY =  MTDNAC.MEMBER_ID AND TO_DATE(MT_CP.CREATED_BY_DEVICE) = MTDNAC.SNAPSHOT_DATE
                                     WHERE RECORDTYPE = 'CST_DNAC'       --AND TO_DATE(CREATED_BY_DEVICE) IN  (SELECT DISTINCT SNAPSHOT_DATE FROM MTDNAC )--use dnac cte
                                     group by 1,2,3,4,5
                                    )MNGD_DEV
                               INNER JOIN (SELECT MEMBER_ID,SNAPSHOT_DATE,DATAQUERYNAME,DATAATTR1,DATAATTR2 FROM NCP_DATA
                                           WHERE DATAQUERYNAME ='inventory_device_detail_2' AND  UPPER(DATAATTR2) = 'MANAGED'
                                    ) AS NCP_INVENTORY_DATA
                                ON MNGD_DEV.INVENTORYDEVICEID_DEVICEID = NCP_INVENTORY_DATA.DATAATTR1  AND TO_DATE(MNGD_DEV.CREATED_BY_DEVICE) = NCP_INVENTORY_DATA.SNAPSHOT_DATE
                                GROUP BY 1,2,3,4
                            ) M
                     INNER JOIN (SELECT DISTINCT TELEMETRY_ID,
									  END_CUSTOMER_PARTY_KEY,
									  RULE_NAME ,
									  SRC_IDENTIFIER
								 FROM {{ source('edw_telmtry_br_db_br', 'n_installed_prod_telemetry') }} LEAN
								 INNER JOIN MTDNAC ON MTDNAC.MEMBER_ID = LEAN.TELEMETRY_ID
								 WHERE TEL_RECORD_TYPE = 'CST_DNAC'
									 AND THREADING_CD = 'DIRECT_CR'
									 AND RULE_NAME = 'MDSN'
									 AND ACTIVE_FLAG='Y'
									 AND SOURCE_DELETED_FLG ='N'
							   ) LEAN ON  M.MEMBER_ID = LEAN.TELEMETRY_ID AND  M.SERIAL_NUMBER = LEAN.SRC_IDENTIFIER --Change_log_4
                     INNER JOIN (SELECT BK_PRODUCT_ID,LEVEL5_COMP_NAME from {{ source('edw_ref_br_db_br_view', 'mt_cx_solution_hierarchy') }}
                                 WHERE LEVEL3_COMP_KEY in (14,15,16,17,18,130,131,132,133,147)
                                ) CX_SOL ON M.PID=CX_SOL.BK_PRODUCT_ID
                     INNER JOIN PRODUCT  ON M.PID = PRODUCT.BK_PRODUCT_ID  WHERE PRODUCT.BK_BUSINESS_UNIT_ID = 'IOTBU'
                     GROUP BY MEMBER_ID, SNAPSHOT_DATE, LEVEL5_COMP_NAME
                    )
                    PIVOT (SUM(SERIAL_NUMBER_CNT) FOR LEVEL5_COMP_NAME IN( 'Switches'
                                                                          ,'Routers'
                                                                          ,'Access Point'
                                                                         )
                                                                    ) AS P (
                                                                            MEMBER_ID
                                                                           ,SNAPSHOT_DATE
                                                                           ,INVENTORY_IOT_SWITCHES_COUNT
                                                                           ,INVENTORY_IOT_ROUTERS_COUNT
                                                                           ,INVENTORY_IOT_ACCS_PNT_COUNT
                                                                           )
)--SELECT * FROM IOTBU_INV_AGG;
 --added for CRs - 973,974,975,976,977
,IOTBU_SITE_AGG AS (SELECT MEMBER_ID
                         ,SNAPSHOT_DATE
                         ,COALESCE(IOT_SWITCHES_ASSIGN_TO_SITE_COUNT,0) AS IOT_SWITCHES_ASSIGN_TO_SITE_COUNT -- 4.	# of industrial switches assigned to site
                         ,COALESCE(IOT_ROUTERS_ASSIGN_TO_SITE_COUNT,0) AS IOT_ROUTERS_ASSIGN_TO_SITE_COUNT   --5.	# of industrial routers assigned to site
                         ,COALESCE(IOT_ACCS_PNT_ASSIGN_TO_SITE_COUNT,0) AS IOT_ACCS_PNT_ASSIGN_TO_SITE_COUNT --6.	# of industrial APs assigned to site
                    FROM
                      (SELECT NCP_SITE_MEMBER_DATA.MEMBER_ID
                            ,NCP_SITE_MEMBER_DATA.SNAPSHOT_DATE
                            ,CX_SOL.LEVEL5_COMP_NAME
                            ,COUNT(DISTINCT NCP_SITE_MEMBER_DATA.DATAATTR2) AS INSTANCE_UUID_CNT
                      FROM (
							SELECT NCP_SITE_DATA.MEMBER_ID
								  ,NCP_SITE_DATA.SNAPSHOT_DATE
								  ,NCP_SITE_DATA.DATAATTR2
								  ,NCP_SITE_DATA.DATAATTR4
							FROM NCP_DATA NCP_SITE_DATA
							INNER JOIN NCP_DATA NCP_INVENTORY_DATA
								ON NCP_SITE_DATA.DATAATTR2 =NCP_INVENTORY_DATA.DATAATTR1
							WHERE NCP_SITE_DATA.DATAQUERYNAME ='site_member_details2'
							AND NCP_INVENTORY_DATA.DATAQUERYNAME ='inventory_device_detail_2'
							AND  UPPER(NCP_INVENTORY_DATA.DATAATTR2) = 'MANAGED'
							) AS NCP_SITE_MEMBER_DATA
                      INNER JOIN (SELECT DISTINCT BK_PRODUCT_ID,LEVEL5_COMP_NAME from {{ source('edw_ref_br_db_br_view', 'mt_cx_solution_hierarchy') }} WHERE LEVEL3_COMP_KEY in (14,15,16,17,18,130,131,132,133,147)) CX_SOL
                              ON NCP_SITE_MEMBER_DATA.DATAATTR4=CX_SOL.BK_PRODUCT_ID
                      INNER JOIN  PRODUCT   ON PRODUCT.BK_PRODUCT_ID=CX_SOL.BK_PRODUCT_ID
                          WHERE PRODUCT.BK_BUSINESS_UNIT_ID = 'IOTBU'
                      GROUP BY MEMBER_ID,SNAPSHOT_DATE,LEVEL5_COMP_NAME
                      )
                   PIVOT(SUM(INSTANCE_UUID_CNT) FOR LEVEL5_COMP_NAME IN('Switches'
                                                                        ,'Routers'
                                                                        ,'Access Point'
                                                                        )
                                                                  ) AS P (MEMBER_ID
                                                                         ,SNAPSHOT_DATE
                                                                         ,IOT_SWITCHES_ASSIGN_TO_SITE_COUNT
                                                                         ,IOT_ROUTERS_ASSIGN_TO_SITE_COUNT
                                                                         ,IOT_ACCS_PNT_ASSIGN_TO_SITE_COUNT
                                                                         )

) --SELECT * FROM IOTBU_SITE_AGG;
  Select
       MTDNAC.MEMBER_ID
      ,MTDNAC.SNAPSHOT_DATE
      ,MTDNAC.TELEMETRY_LAST_COLLECTED_ON_DATE
      ,COALESCE(NCP_LIC.SWITCHES_HUBS_COUNT,0) AS LIC_SWITCHES_COUNT
      ,COALESCE(NCP_LIC.WIRELESS_CONTROLLER_COUNT,0) AS LIC_WLC_COUNT
      ,(COALESCE(NCP_LIC.LIC_TOTAL_MANAGED_DEVICES_COUNT,0)+ COALESCE(ASSURANCE_AGG.AP_COUNT,0))::INTEGER AS LIC_TOTAL_MANAGED_DEVICES_COUNT
      ,COALESCE(NCP_LIC.LIC_ROUTER_COUNT,0) AS LIC_ROUTER_COUNT
      ,COALESCE(NCP_LIC.LIC_OTHER_DEVICE_COUNT,0) AS LIC_OTHER_DEVICE_COUNT
      ,(COALESCE(NCP_LIC.SITE_TOTAL_MANAGED_DEVICES_COUNT,0)+COALESCE(ASSURANCE_AGG.AP_SITE_COUNT,0))::INTEGER AS LIC_VALID_SITE_TOTAL_MANAGED_DEVICES_COUNT
      ,COALESCE(NCP_LIC.SITE_SWITCHES_HUBS,0) AS LIC_VALID_SITE_SWITCHES_COUNT
      ,COALESCE(NCP_LIC.SITE_ROUTER_COUNT,0) AS LIC_VALID_SITE_ROUTER_COUNT
      ,COALESCE(NCP_LIC.SITE_WIRELESS_CONTROLLER,0) AS LIC_VALID_SITE_WLC_COUNT
      ,COALESCE(NCP_LIC.SITE_OTHER_DEVICE_COUNT,0) AS LIC_VALID_SITE_OTHER_DEVICE_COUNT
      ,COALESCE(NCP_AGG.TOTAL_WIRED_CLIENT_DEVICES,0) AS MD_TOTAL_WIRED_CLIENT_DEVICES
      ,COALESCE(NCP_AGG.TOTAL_WIRELESS_CLIENT_DEVICES,0) AS MD_TOTAL_WIRELESS_CLIENT_DEVICES
      ,(COALESCE(NCP_AGG.TOTAL_WIRED_CLIENT_DEVICES,0)+ COALESCE(NCP_AGG.TOTAL_WIRELESS_CLIENT_DEVICES,0))::INTEGER AS MD_TOTAL_WIRED_WIRELESS_DEVICES
      ,COALESCE(MT_NCP.SDA_VN_COUNT,0) AS SDA_VIRTUAL_NETWORK_COUNT
      ,COALESCE(MT_NCP.SDA_FABRIC_SSID_COUNT,0) AS SDA_FABRIC_SSID_COUNT
      ,MT_NCP.SDA_DEVICES_COUNT
      ,NCP.GOLDEN_TAG_IMAGE_COUNT
      ,NCP.DEVICE_CONTROLLABILITY_ENABLED_FLAG
      ,NCP_NON.NON_GLOBAL_SITE_COUNT
      ,NCP.SCALABLE_GROUPS_COUNT
      ,NCP.ACCESS_POLICY_CONTRACT_COUNT
      ,NCP.ACCESS_GRP_POLICY_COUNT
      ,COALESCE(NCP_ISE.ISE_INTEGRATION_FLAG,'N') AS ISE_INTEGRATION_FLAG
      ,NCP_AGG.SDA_SWITCH_COUNT
      ,NCP_AGG.SDA_ROUTER_COUNT
      ,NCP_AGG.SDA_WLC_COUNT
      ,NCP.SDA_ACCESS_POINT_COUNT
      ,NCP_SP.SDA_AP_COLLECTED_ON_DT
      ,NCP.ONBOARDING_TEMPLATE_COUNT
      ,NCP.NETWRK_PRFL_ASSCTDTO_SITE_COUNT
      ,NCP_AGG.PORTS_EASY_CONNECT
      ,NCP_AGG.PORTS_CLOSED_AUTH
      ,NCP_AGG.PORTS_NO_AUTH
      ,NCP_AGG.PORTS_OPEN_AUTH
      ,NCP_AGG.NTWRK_DEV_ASGN_TO_SITE_CNT
      ,NCP_INVENTORY_DEVICE_DTL_CNT.INV_DEVICE_DTL_CNT --Change_log_4
      ,NCP_AGG.DV_SDA_FABRIC_LAN_SITE_DOMAIN_CNT
      ,NCP_AGG.DV_SDA_FABRIC_SITE_DOMAIN_CNT
      ,NCP_AGG.DV_SDA_TRANSIT_SITE_DOMAIN_CNT
      ,NCP.DV_TMPLTES_PROVSNED_APPL_DVC_CNT
      ,NCP.DV_TMPLTES_PROVSNED_MNGD_DVC_CNT
  	  ,COALESCE(NCP_SWIM_CTE.SWIM_UPGRADED_SWITCH_CNT, 0) AS SWIM_UPGRADED_SWITCH_CNT
	  ,COALESCE(NCP_SWIM_CTE.SWIM_UPGRADED_WLC_CNT, 0) AS SWIM_UPGRADED_WLC_CNT
	  ,COALESCE(NCP_SWIM_CTE.SWIM_UPGRADED_ROUTERS_CNT, 0) AS SWIM_UPGRADED_ROUTERS_CNT
      ,MT_NCP.DV_SDA_CNTRL_NODE_DEVICE_CNT
      ,MT_NCP.DV_SDA_EDGE_NODE_DEVICE_CNT
      ,MT_NCP.DV_SDA_BORDER_NODE_DEVICE_CNT
      ,MT_NCP.DV_SDA_CONCTD_TO_FBRC_CLNTS_CNT
      ,MT_NCP.DV_SDA_FABRIC_IP_POOLS_COUNT
      --,MT_NCP.DV_SDA_WIRD_CLNTS_CNTD_FBRC_CNT --Change_log_3
      --,MT_NCP.DV_SDA_WIRLSS_CLNTS_CNTD_FBRC_CNT --Change_log_1
      ,COALESCE(NCP_AGG.MACHINE_REASNG_CNT,0) AS MACHINE_REASNG_CNT
      ,CASE WHEN TO_DATE(MTDNAC.SNAPSHOT_DATE)=(SELECT MAX_SNAPSHOT_DT FROM MAXDT) THEN 'Y' ELSE 'N' END AS CURRENT_FLAG
        	  -- below 6 metrics for  CRs - 973,974,975,976,977
	  ,COALESCE(IOTBU_INV_AGG.INVENTORY_IOT_SWITCHES_COUNT, 0) AS INVENTORY_IOT_SWITCHES_COUNT
      ,COALESCE(IOTBU_INV_AGG.INVENTORY_IOT_ROUTERS_COUNT, 0) AS INVENTORY_IOT_ROUTERS_COUNT
      ,COALESCE(IOTBU_INV_AGG.INVENTORY_IOT_ACCS_PNT_COUNT, 0) AS INVENTORY_IOT_ACCS_PNT_COUNT
      ,COALESCE(IOTBU_SITE_AGG.IOT_SWITCHES_ASSIGN_TO_SITE_COUNT, 0) AS IOT_SWITCHES_ASSIGN_TO_SITE_COUNT
      ,COALESCE(IOTBU_SITE_AGG.IOT_ROUTERS_ASSIGN_TO_SITE_COUNT, 0) AS IOT_ROUTERS_ASSIGN_TO_SITE_COUNT
      ,COALESCE(IOTBU_SITE_AGG.IOT_ACCS_PNT_ASSIGN_TO_SITE_COUNT, 0) AS IOT_ACCS_PNT_ASSIGN_TO_SITE_COUNT
      ,COALESCE(NCP_INVENTORY_DEVICE_DTL_CNT.INVENTORY_SWITCHES_CNT, 0) AS INVENTORY_SWITCHES_CNT
	  ,COALESCE(NCP_INVENTORY_DEVICE_DTL_CNT.INVENTORY_ROUTERS_CNT, 0) AS INVENTORY_ROUTERS_CNT
	  ,COALESCE(NCP_INVENTORY_DEVICE_DTL_CNT.INVENTORY_ACCESS_POINT_CNT, 0) AS INVENTORY_ACCESS_POINT_CNT
	  ,COALESCE(NCP_INVENTORY_DEVICE_DTL_CNT.INVENTORY_WLC_CNT, 0) AS INVENTORY_WLC_CNT
	  ,COALESCE(NCP_AGG.VIRTUAL_ACCOUNT_LIST ,'NA') AS VIRTUAL_ACCOUNT_LIST
	  ,COALESCE(NCP_AGG.SMU_UPGRADES_COMPLETED_CNT ,0) AS SMU_UPGRADES_COMPLETED_CNT
	  ,COALESCE(NCP_SITE_MEMBER.SWITCHES_ASSIGNED_TO_SITE_CNT ,0) AS SWITCHES_ASSIGNED_TO_SITE_CNT
	  ,COALESCE(NCP_SITE_MEMBER.ROUTERS_ASSIGNED_TO_SITE_CNT ,0) AS ROUTERS_ASSIGNED_TO_SITE_CNT
	  ,COALESCE(NCP_SITE_MEMBER.WLC_ASSIGNED_TO_SITE_CNT ,0) AS WLC_ASSIGNED_TO_SITE_CNT
	  ,COALESCE(NCP_SITE_MEMBER.AP_ASSIGNED_TO_SITE_CNT ,0) AS AP_ASSIGNED_TO_SITE_CNT
	  ,COALESCE(NCP_AGG.UMBRELLA_INTEGRATN_ENABLED_DEVICE_CNT ,0) AS UMBRELLA_INTEGRATN_ENABLED_DEVICE_CNT
      ,COALESCE(INVENTORY_SERIAL_NBR_CAT9K_CNT.INVENTORY_9K_SWITCHES_CNT, 0) AS INVENTORY_9K_SWITCHES_CNT -- Change_log_5
      ,COALESCE(NCP_AGG.NETWRK_PRFL_NAMESPACE_COUNT,0) AS NETWORK_PROFILES_CREATED_CNT
	  ,COALESCE(NCP_AGG.SECURITY_VULNERAB_SCAN,0) AS SECURITY_VULNERABILITY_SCANS_CNT
      ,NCP_AGG.PORTS_LOW_IMPACT_CNT AS PORTS_LOW_IMPACT_CNT
      ,COALESCE(NCP_AGG.RMA_WORKFLOWS_COMPLETED_CNT, 0) AS RMA_WORKFLOWS_COMPLETED_CNT
      FROM MTDNAC
LEFT JOIN NCP        ON MTDNAC.MEMBER_ID = NCP.MEMBER_ID        AND MTDNAC.SNAPSHOT_DATE = NCP.SNAPSHOT_DATE
LEFT JOIN NCP_AGG    ON MTDNAC.MEMBER_ID = NCP_AGG.MEMBER_ID    AND MTDNAC.SNAPSHOT_DATE = NCP_AGG.SNAPSHOT_DATE
LEFT JOIN NCP_SP    ON MTDNAC.MEMBER_ID = NCP_SP.MEMBER_ID    AND MTDNAC.SNAPSHOT_DATE = NCP_SP.SNAPSHOT_DATE
LEFT JOIN NCP_ISE    ON MTDNAC.MEMBER_ID = NCP_ISE.MEMBER_ID    AND MTDNAC.SNAPSHOT_DATE = NCP_ISE.SNAPSHOT_DATE
LEFT JOIN NCP_NON    ON MTDNAC.MEMBER_ID = NCP_NON.MEMBER_ID    AND MTDNAC.SNAPSHOT_DATE = NCP_NON.SNAPSHOT_DATE
LEFT JOIN ASSURANCE_AGG  ON MTDNAC.MEMBER_ID = ASSURANCE_AGG.MEMBER_ID  AND MTDNAC.SNAPSHOT_DATE = ASSURANCE_AGG.SNAPSHOT_DATE -- Changed by balaji
LEFT JOIN MT_NCP     ON MTDNAC.MEMBER_ID = MT_NCP.MEMBER_ID     AND MTDNAC.SNAPSHOT_DATE = MT_NCP.SNAPSHOT_DATE
LEFT JOIN NCP_LIC    ON MTDNAC.MEMBER_ID = NCP_LIC.MEMBER_ID    AND MTDNAC.SNAPSHOT_DATE = NCP_LIC.SNAPSHOT_DATE
LEFT JOIN NCP_SWIM_CTE ON MTDNAC.MEMBER_ID = NCP_SWIM_CTE.MEMBER_ID AND MTDNAC.SNAPSHOT_DATE = NCP_SWIM_CTE.SNAPSHOT_DATE
LEFT JOIN IOTBU_INV_AGG  ON MTDNAC.MEMBER_ID = IOTBU_INV_AGG.MEMBER_ID AND MTDNAC.SNAPSHOT_DATE = IOTBU_INV_AGG.SNAPSHOT_DATE
LEFT JOIN IOTBU_SITE_AGG  ON MTDNAC.MEMBER_ID= IOTBU_SITE_AGG.MEMBER_ID AND MTDNAC.SNAPSHOT_DATE = IOTBU_SITE_AGG.SNAPSHOT_DATE
LEFT JOIN NCP_INVENTORY_DEVICE_DTL_CNT  ON MTDNAC.MEMBER_ID = NCP_INVENTORY_DEVICE_DTL_CNT.MEMBER_ID
AND MTDNAC.SNAPSHOT_DATE = NCP_INVENTORY_DEVICE_DTL_CNT.SNAPSHOT_DATE -- Change_log_4
--LEFT JOIN NCP_SWIM_LEAN  ON MTDNAC.MEMBER_ID = NCP_SWIM_LEAN.MEMBER_ID AND MTDNAC.SNAPSHOT_DATE = NCP_SWIM_LEAN.SNAPSHOT_DATE -- Change_log_4
LEFT JOIN NCP_SITE_MEMBER ON MTDNAC.MEMBER_ID = NCP_SITE_MEMBER.MEMBER_ID  AND MTDNAC.SNAPSHOT_DATE = NCP_SITE_MEMBER.SNAPSHOT_DATE -- Change_log_4
LEFT JOIN INVENTORY_SERIAL_NBR_CAT9K_CNT  ON MTDNAC.MEMBER_ID = INVENTORY_SERIAL_NBR_CAT9K_CNT.MEMBER_ID
AND MTDNAC.SNAPSHOT_DATE = INVENTORY_SERIAL_NBR_CAT9K_CNT.SNAPSHOT_DATE -- Change_log_5
