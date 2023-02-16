CREATE OR REPLACE FUNCTION public.fnc_consultar_parametro(i_codigo character varying)
 RETURNS text
 LANGUAGE plpgsql
AS $function$

	declare 
		v_fecha_hora_inicio				timestamp;
		v_fecha_hora_fin				timestamp;
		v_duracion_proceso				varchar(50) := '';

		v_valor							text;
		v_nombre_up						varchar(100) := 'fnc_consultar_parametro';
		v_texto_log						text;
		v_state   						text;
		v_msg   						text;
		v_detail   						text;
		v_hint   						text;
		v_contex   						text;
		v_error   						text;
	begin 
		v_texto_log := 'Entro. i_codigo: ' || i_codigo;
		insert into logs (nombre_up, texto_log) values (v_nombre_up, v_texto_log);
		raise notice 'fnc_consultar_parametro %', v_texto_log;
	
		v_fecha_hora_inicio := clock_timestamp();	
		---------------------------------------------------------------------------------------------------------------------	
		select valor
		  into v_valor
		  from wacher_parametros wp 
		 where codigo = i_codigo
		limit 1
		;
		raise notice 'v_valor %', v_valor;
		insert into logs (nombre_up, texto_log) values (v_nombre_up, 'v_valor: ' || v_valor);
		---------------------------------------------------------------------------------------------------------------------	
		v_fecha_hora_fin := clock_timestamp();
		v_duracion_proceso	:= age(v_fecha_hora_fin, v_fecha_hora_inicio);
			
		v_texto_log := 'Salio.  Hora Inicio: ' || v_fecha_hora_inicio  || ' Hora Fin: ' || v_fecha_hora_fin  || ' Duración: ' || v_duracion_proceso;
		insert into logs (nombre_up, texto_log) values (v_nombre_up, v_texto_log);
		return v_valor;
	exception
		when others then
			get STACKED diagnostics  
	        	v_state   = RETURNED_SQLSTATE,
				v_msg     = MESSAGE_TEXT,
				v_detail  = PG_EXCEPTION_DETAIL,
				v_hint    = PG_EXCEPTION_HINT;
				v_error	:= v_state || ' ' || v_msg || ' ' || v_detail || ' ' || v_hint;
	           	insert into logs (nombre_up, texto_log) values (v_nombre_up, v_error);
	           	raise notice 'fnc_consultar_parametro %', v_error;
	end;
$function$
;
