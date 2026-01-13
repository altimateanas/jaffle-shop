{{ config(
    query_tag='cisco_demo'
) }}

select
    QL.SERVICE_QUOTE_LINE_KEY,
    IB.SK_INSTANCE_ID_INT,
    CL.SK_ID_LINT as CONTRACT_LINE_ID_INT,
    QL.BK_QUOTE_NUM,
    QL.SK_QUOTE_LINE_ID_INT,
    QL.SQ_LN_STATUS_CD,
    QL.SQ_LN_START_DT,
    QL.SQ_LN_END_DT,
    QL.ATTRITION_CD,
    QL.SERVICE_PRODUCT_KEY,
    QL.DV_PRICING_SKU_ID,
    QL.SQ_LN_NET_TTL_PRC_USD_AMT,
    QL.SALES_ORDER_LINE_KEY,
    QL.SQ_LINE_SERVICE_ORDER_NUM,
    QL.SS_CD,
    to_timestamp(QL.EDW_CREATE_DTM) as EDWSF_CREATE_DTM,
    QL.EDW_CREATE_USER as EDWSF_CREATE_USER,
    to_timestamp(QL.EDW_UPDATE_DTM) as EDWSF_UPDATE_DTM,
    QL.EDW_UPDATE_USER as EDWSF_UPDATE_USER,
    'N' as EDWSF_SOURCE_DELETED_FLAG
from {{ source('edw_cafe_db_pv', 'pv_n_service_quote_line_stream_hana') }} QL
left join {{ source('edw_cafe_db_pv', 'pv_installed_product') }} IB
    on QL.IP_KEY = IB.IP_KEY
left join {{ source('edw_cafe_db_pv', 'pv_svc_cntrct_ln_tech_services') }} CL
    on QL.PRR_SVC_CNTRCT_LN_TEC_SVCS_KEY = CL.SVC_CNTRCT_LN_TECH_SVCS_KEY
where QL.SS_CD = 'CCWR'
    and QL.METADATA$ACTION = 'INSERT'
    and to_timestamp(QL.EDW_UPDATE_DTM) >= to_timestamp('2025-09-30 00:00:00')
    and CL.SK_ID_LINT not in ('124068311089','126775803218','128314779459')
