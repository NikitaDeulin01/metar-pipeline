select 1
from {{ ref('stg_metar_observations') }}
where visibility_m < 0
