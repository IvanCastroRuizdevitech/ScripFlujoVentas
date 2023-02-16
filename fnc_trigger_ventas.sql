CREATE OR REPLACE FUNCTION public.fnc_trigger_ventas()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
declare

	v_id_tipo_venta					int8;
	v_id_tipo_venta_transacciones	int8;
	v_codigo_sap_transacciones		int8;
	v_id_medio_pago					int8;
	
	v_indicador_remision			varchar(1) 	:= 'N';
	v_id_tipo_venta_ventas			text;

	v_ind_contingencia				bool		:= false;

	v_respuesta						json;
	v_nombre_up						text	:= 'fnc_trigger_ventas';
	v_texto_log						text;
	v_state   						text;
	v_msg   						text;
    v_detail   						text;
    v_hint   						text;
    v_contex   						text;
  	v_error   						text;	

begin
	
	if (tg_op = 'INSERT') then
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'ENTRO - INSERT', new.id);
		raise notice '% Entro a insert', v_nombre_up;
	
		select (new.atributos ->> 'tipoVenta')::text,  (new.atributos ->> 'contingencia')::bool into v_id_tipo_venta_ventas, v_ind_contingencia;
		v_texto_log	:= 'new.id ' || new.id || ' Tipo Venta - Venta: ' || coalesce(v_id_tipo_venta::text, '');
		insert into logs (nombre_up, texto_log, id_proceso)  values (v_nombre_up, v_texto_log, new.id);
		
		
		if v_id_tipo_venta_ventas = '001' then 
			v_id_tipo_venta := 6;
		elsif v_id_tipo_venta_ventas = '002' and v_ind_contingencia then 
			v_id_tipo_venta := 7;
		elsif v_id_tipo_venta_ventas is null then				
			select (t.trama::json ->> 'tipoVenta')::int id_tipo_venta,  (t.trama::json -> 'response'->> 'identificadorFormaPago')::int codigo_sap
			  into v_id_tipo_venta_transacciones, v_codigo_sap_transacciones
			  from transacciones  t
			 where new.token_process_id = t.id;
			v_texto_log := 	'v_id_tipo_venta_transacciones ' || coalesce(v_id_tipo_venta_transacciones::text,'') || ' v_codigo_sap_transacciones: ' || coalesce(v_codigo_sap_transacciones::text,'');
			insert into logs (nombre_up, texto_log, id_proceso)  values (v_nombre_up, v_texto_log, new.id);
		
			if (v_id_tipo_venta_transacciones = 3 or v_codigo_sap_transacciones is not null) then
				v_id_tipo_venta := coalesce(v_id_tipo_venta_transacciones, 4);
			elsif v_id_tipo_venta_transacciones is not null then
				v_id_tipo_venta := v_id_tipo_venta_transacciones;
			else 
				v_id_tipo_venta := -1;
			end if;
		else
			v_id_tipo_venta := v_id_tipo_venta_ventas::int;
		end if;
		
		-- Se consulta el medio de pago de acuerdo al codigo sap si la venta es tipo rumbo
		if v_id_tipo_venta = 4 and v_codigo_sap_transacciones is not null then
			select id
			  into v_id_medio_pago
			 from medios_pagos mp 
			where (mp_atributos ->> 'codigoSAP')::int = v_codigo_sap_transacciones;
			if v_id_medio_pago is null then
				v_id_medio_pago := 1;
			end if;
		end if;
	
		new.id_tipo_venta 	:= v_id_tipo_venta::int;
		new.medios_pagos_id := v_id_medio_pago;
		new.codigo_sap 		:= v_codigo_sap_transacciones::int;
		
		v_texto_log := 	'v_id_tipo_venta ' || coalesce(v_id_tipo_venta::text,'') || ' v_id_medio_pago: ' || coalesce(v_id_medio_pago::text,'');
		insert into logs (nombre_up, texto_log, id_proceso)  values (v_nombre_up, v_texto_log, new.id);
	
		insert into logs (nombre_up, texto_log, id_proceso)  values (v_nombre_up, 'SALIO - INSERT', new.id);
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
			insert into logs (nombre_up, texto_log, id_proceso)  values (v_nombre_up, v_error, new.id);
			return new;
end;
$function$
;
