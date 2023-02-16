CREATE OR REPLACE FUNCTION public.fnc_trigger_ventas_medios_pago()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
declare

	r_ventas						record;
	r_venta_medio_pagos				record;
	
	v_json_medios_pago				json;
	
	v_codigo_respuesta				text;
	v_nombre_up						text	:= 'fnc_trigger_ventas_medios_pago';
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
		
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'new.id ' || new.id || ' ventas_id: ' || new.ventas_id,  new.ventas_id);
		
		-- Se consulta la información de la venta
		select v.id_tipo_venta, cm.id id_movimiento, cm.venta_total
		  into r_ventas
		  from ventas v
		  join ct_movimientos cm on cm.remoto_id = v.id
		 where v.id = new.ventas_id;
		 
		v_texto_log := 'r_ventas.id_tipo_venta ' || r_ventas.id_tipo_venta || ' r_ventas.id_movimiento ' || r_ventas.id_movimiento || ' r_ventas.venta_total ' || r_ventas.venta_total;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, new.ventas_id);
		raise notice '%  %', v_nombre_up, v_texto_log;
		 
		if r_ventas.id_tipo_venta = 6 or r_ventas.id_tipo_venta = 7 then
			-- Se valida si la venta ya tiene medio de pagos asociados
			 select count(id) cantidad_medios_pago, sum(valor_total) valor_total_medios_pago
			 into r_venta_medio_pagos
			  from ventas_medios_pagos vmp 
			 where vmp.ventas_id =  new.ventas_id
			  and vmp.id != new.id;
			
			r_venta_medio_pagos.valor_total_medios_pago := coalesce(r_venta_medio_pagos.valor_total_medios_pago,0);
			r_venta_medio_pagos.cantidad_medios_pago := coalesce(r_venta_medio_pagos.cantidad_medios_pago,0);		
			 
			v_texto_log := 'cantidad_medios_pago ' || coalesce(r_venta_medio_pagos.cantidad_medios_pago,0) || ' valor_total_medios_pago ' || coalesce(r_venta_medio_pagos.valor_total_medios_pago,0)  || ' new.valor_total ' || coalesce(new.valor_total,0);
			insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, new.ventas_id);
			raise notice '%  %', v_nombre_up, v_texto_log;
		
			v_texto_log := '(r_venta_medio_pagos.valor_total_medios_pago +  new.valor_total) ' || coalesce((r_venta_medio_pagos.valor_total_medios_pago +  new.valor_total),0) || ' r_ventas.venta_total ' || coalesce(r_ventas.venta_total,0) ;
			insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, new.ventas_id);
			raise notice '%  %', v_nombre_up, v_texto_log;
		
			v_texto_log := 'r_venta_medio_pagos.valor_total_medios_pago <= r_ventas.venta_total '|| (r_venta_medio_pagos.valor_total_medios_pago <= r_ventas.venta_total) 
						|| '(r_venta_medio_pagos.valor_total_medios_pago +  new.valor_total) <=  r_ventas.venta_total) ' || ((r_venta_medio_pagos.valor_total_medios_pago +  new.valor_total) <=  r_ventas.venta_total);
			insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, new.ventas_id);
			raise notice '%  %', v_nombre_up, v_texto_log;
			
			if (r_venta_medio_pagos.valor_total_medios_pago <= r_ventas.venta_total)
				and ((r_venta_medio_pagos.valor_total_medios_pago +  new.valor_total) <=  r_ventas.venta_total) then
				-- Se inserta en ct_movimientos_medios_pago
				begin
					insert into ct_movimientos_medios_pagos (ct_medios_pagos_id,		ct_movimientos_id,				valor_recibido,			valor_cambio
														   , valor_total,				numero_comprobante,				moneda_local,			trm)
													 values (new.medios_pagos_id,		r_ventas.id_movimiento,			new.valor_recibido,		new.valor_cambio
														   , new.valor_total,			new.numero_comprobante,			'S',					0);	
														   
					select array_to_json(array_agg((row_to_json(pagos))))pagos
					  into v_json_medios_pago
					  from (
							select vmp.medios_pagos_id 			medio
								 , mp.descripcion 				descripcion
								 , vmp.valor_recibido 			valor_recibido
								 , vmp.valor_cambio 			valor_cambio
								 , vmp.numero_comprobante		numero_comprobante
							  from ventas_medios_pagos vmp
							  join medios_pagos mp on mp.id = vmp.medios_pagos_id
							 where ventas_id  		= new.ventas_id
						   ) pagos;

					v_texto_log := 'v_json_medios_pago ' || coalesce(v_json_medios_pago::text,'');
					insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, new.ventas_id);
					raise notice '%  %', v_nombre_up, v_texto_log;
						   
					update ct_movimientos
					  set json_data = jsonb_set(json_data::jsonb, '{medios_pagos}',to_jsonb(v_json_medios_pago) )
					where id = r_ventas.id_movimiento;

					v_texto_log := 'Despues de actualizar el json de medio de pagos en movimientos ';
					insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, new.ventas_id);
					raise notice '%  %', v_nombre_up, v_texto_log;
				exception	  
						when others then
						get STACKED diagnostics  
							v_state   = RETURNED_SQLSTATE,
							v_msg     = MESSAGE_TEXT,
							v_detail  = PG_EXCEPTION_DETAIL,
							v_hint    = PG_EXCEPTION_HINT;
							v_error	:= 'Insert ct_movimientos_medios_pagos ' || v_state || ' ' || v_msg || ' ' || v_detail || ' ' || v_hint;
							v_codigo_respuesta 	:= 'ERROR';
							insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_codigo_respuesta || ' ' || v_error, new.ventas_id);
							raise notice '% %', v_nombre_up, v_codigo_respuesta || ' ' || v_error;
							return new;
				end; -- Fin  ct_movimientos_medios_pagos
			else
				v_texto_log := 'El total de medio de pago supera el total de la venta' ;
				insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, new.ventas_id);
				raise notice '%  %', v_nombre_up, v_texto_log;
				return new;
			end if; -- Fin if 			
		end if;

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