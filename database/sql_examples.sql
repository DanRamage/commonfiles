#Get the obs and uom names.
select distinct platform.platform_handle,obs_type.standard_name, uom_type.standard_name
from platform
       left join sensor on platform.row_id=sensor.platform_id
       left join m_type on m_type.row_id=sensor.m_type_id
       left join m_scalar_type on m_scalar_type.row_id=m_type.m_scalar_type_id
       left join obs_type on obs_type.row_id=m_scalar_type.obs_type_id
       left join uom_type on uom_type.row_id=m_scalar_type.uom_type_id
where platform_handle like 'lbhmc.Apache.pier'
order by platform_handle,obs_type.standard_name;