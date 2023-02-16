CREATE OR REPLACE FUNCTION public.fnc_validar_venta_credito(i_id_venta bigint)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$

	declare 
		v_fecha_hora_inicio				timestamp;
		v_fecha_hora_fin				timestamp;
		v_duracion_proceso				varchar(50) := '';
	
		r_info_venta_transacion			record;
		v_respuesta						bool	:= false;

		v_nombre_up						varchar(100) := 'fnc_validar_venta_credito';
		v_texto_log						text;
		v_state   						text;
		v_msg   						text;
		v_detail   						text;
		v_hint   						text;
		v_contex   						text;
		v_error   						text;
	begin 
		v_texto_log := 'Entro. i_id_venta: ' || i_id_venta;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
		raise notice 'fnc_validar_venta_credito %', v_texto_log;
	
		v_fecha_hora_inicio := clock_timestamp();	
		---------------------------------------------------------------------------------------------------------------------	
		
		select coalesce(t.proveedores_id, 0) 					id_proveedor
			 , coalesce((t.trama::json -> 'tipoVenta')::text,'0')::int	tipo_venta
		  into r_info_venta_transacion
		  from ventas v 
		  join transacciones t on v.token_process_id = t.id
		 where v.id = i_id_venta
		  ;
		
		 if r_info_venta_transacion.id_proveedor = 3 or r_info_venta_transacion.tipo_venta = '10' then 
		 	v_respuesta := true;
		 else
		 	v_respuesta := false;
		 end if;
		 
		---------------------------------------------------------------------------------------------------------------------	
		v_fecha_hora_fin := clock_timestamp();
		v_duracion_proceso	:= age(v_fecha_hora_fin, v_fecha_hora_inicio);
			
		v_texto_log := 'Salio. i_id_venta: ' || i_id_venta || ' Hora Inicio: ' || v_fecha_hora_inicio  || ' Hora Fin: ' || v_fecha_hora_fin  || ' Duración: ' || v_duracion_proceso;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
		return v_respuesta;
	exception
		when others then
			get STACKED diagnostics  
	        	v_state   = RETURNED_SQLSTATE,
				v_msg     = MESSAGE_TEXT,
				v_detail  = PG_EXCEPTION_DETAIL,
				v_hint    = PG_EXCEPTION_HINT;
				v_error	:= v_state || ' ' || v_msg || ' ' || v_detail || ' ' || v_hint;
	           	insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_error, i_id_venta);
	           	raise notice '%', v_error;
	           	return false;
	end;
$function$
;
