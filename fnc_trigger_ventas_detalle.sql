CREATE OR REPLACE FUNCTION public.fnc_trigger_ventas_detalle()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
declare
	v_id_tipo_venta					int8 := -1;
	v_id_tipo_venta_transacciones	int8;
	
	v_json_respuesta				json := '{}';
	
	v_respuesta						json;
	v_nombre_up						text	:= 'fnc_trigger_ventas_detalle';
	v_texto_log						text;
	v_state   						text;
	v_msg   						text;
    v_detail   						text;
    v_hint   						text;
    v_contex   						text;
  	v_error   						text;	

begin
	
	if (tg_op = 'INSERT') then
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'ENTRO - INSERT',  new.id);
		raise notice '% Entro a insert', v_nombre_up;
		
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'new.id ' || new.id || ' ventas_id: ' || new.ventas_id,  new.id);
		
		call prc_registro_movimiento (i_id_venta => new.ventas_id, o_json_respuesta => v_json_respuesta);
		
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_respuesta ' || v_json_respuesta,  new.id);

		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'SALIO - INSERT',  new.id);
		raise notice '% SALIO a insert', v_nombre_up;
		return new;
	end if; -- FIn si es Insert
exception
	when others then
		GET STACKED DIAGNOSTICS 
			v_state   = RETURNED_SQLSTATE,
            v_msg     = MESSAGE_TEXT,
            v_detail  = PG_EXCEPTION_DETAIL,
            v_hint    = PG_EXCEPTION_HINT;
			v_error	:= v_state || ' ' || v_msg || ' ' || v_detail || ' ' || v_hint;
			insert into logs (nombre_up, texto_log) values (v_nombre_up, v_error);
			return new;
end;
$function$
;