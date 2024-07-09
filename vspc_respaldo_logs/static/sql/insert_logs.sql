insert into {}_hist partition (year)
        select
        ingestion_year,
        ingestion_month,
        ingestion_day ,
        i,
        n ,
        documento ,
        tipo ,
        nombre ,
        estado ,
        to_timestamp(hora_inicio,"yyyy/MM/dd HH:mm:ss") as hora_inicio ,
        duracion ,
        etapa ,
        sub_i ,
        now() as fecha_carga ,
        cast(cant_registros as string),
        year 
        from {}
        ;