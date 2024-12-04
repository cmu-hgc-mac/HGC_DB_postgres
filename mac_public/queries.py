import asyncpg, asyncio

mod_simple_query = f"""SELECT 
    REPLACE(mpt.module_name,'-','') AS module_name, 
    mpt.status,
    mpt.status_desc,
    mpt.count_bad_cells,
    mpt.bias_vol,
    mpt.trim_bias_voltage FROM module_pedestal_test AS mpt"""

mod_ped_query = f"""SELECT 
    REPLACE(mpt.module_name,'-','') AS module_name, 
    mpt.status,
    mpt.status_desc,
    mpt.bias_vol,
    mpt.trim_bias_voltage,
    mpt.meas_leakage_current,
    mpt.count_bad_cells,
    mpt.list_dead_cells,
    mpt.list_noisy_cells,
    mpt.list_disconnected_cells,
    mpt.comment,
    mi.hxb_name,
    mi.sen_name,
    mi.bp_name
FROM module_pedestal_test mpt JOIN module_info mi
    ON REPLACE(mpt.module_name,'-','') = REPLACE(mi.module_name,'-','');
"""

hxb_ped_query = f"""SELECT 
        REPLACE(hdp.hxb_name,'-','') as hxb_name, 
        REPLACE(mi.module_name,'-','') AS module_name,
        hdp.status,
        hdp.status_desc,
        hdp.bias_vol,
        hdp.trim_bias_voltage,
        hdp.meas_leakage_current,
        hdp.count_bad_cells,
        hdp.list_dead_cells,
        hdp.list_noisy_cells,
        hdp.list_disconnected_cells,
        hdp.comment
    FROM module_pedestal_test hdp JOIN module_info mi
    ON REPLACE(hdp.hxb_name,'-','') = REPLACE(mi.hxb_name,'-','');"""

mod_iv_query = mod_iv_query = f"""SELECT 
    REPLACE(miv.module_name,'-','') AS module_name, 
    miv.status, 
    miv.status_desc, 
    miv.grade, 
    miv.ratio_i_at_vs, 
    miv.ratio_at_vs, 
    miv.comment, 
    miv.rel_hum, 
    miv.temp_c, 
    miv.date_test, 
    miv.time_test, 
    miv.inspector, 
    mi.hxb_name, 
    mi.sen_name,
    mi.bp_name
FROM mod_ivtest miv JOIN module_info mi
    ON miv.module_name = mi.module_name;
"""
