CREATE OR REPLACE FUNCTION public.fnc_validar_venta_electronica(i_id_venta bigint)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$

	declare 
		v_fecha_hora_inicio				timestamp;
		v_fecha_hora_fin				timestamp;
		v_duracion_proceso				varchar(50) := '';	
	
		v_tiene_informacion_fe			int			:= 0;
		v_tipo_venta					int			:= -1;
	
		v_valor_venta					float 		:= 0;
	
		v_ind_modulo_fe_activo			varchar(1) 	:= 'N'; -- indicador de que si el modulo de factuación electronica esta activo
		v_ind_fe_por_defecto			varchar(1) 	:= 'N';
		v_ind_fe_oblgatorio				varchar(1) 	:= 'N';
		v_monto_minimo_fe				varchar(20) := '0';
	
		v_respuesta						bool		:= false;
		v_ind_venta_credito				bool 		:= false;
	
		v_nombre_up						varchar(100) := 'fnc_validar_venta_electronica';
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
		raise notice ' fnc_validar_venta_electronica %', v_texto_log;
	
		v_fecha_hora_inicio := clock_timestamp();	
		---------------------------------------------------------------------------------------------------------------------	
		
		v_ind_venta_credito		:= fnc_validar_venta_credito(i_id_venta => i_id_venta);
		v_ind_modulo_fe_activo	:= fnc_consultar_parametro (i_codigo => 'MODULO_FACTURACION_ELECTRONICA');
		v_ind_fe_por_defecto	:= fnc_consultar_parametro (i_codigo => 'DEFAULT_FE');
		v_ind_fe_oblgatorio		:= fnc_consultar_parametro (i_codigo => 'OBLIGATORIO_FE');
		v_monto_minimo_fe		:= fnc_consultar_parametro (i_codigo => 'MONTO_MINIMO_FE');
	
		select (t.trama::json ->> 'tipoVenta')::int 
		  into v_tipo_venta 
		  from ventas 			v 
	      join transacciones 	t on v.token_process_id = t.id 
		 where v.id = i_id_venta; 
		
		if v_tipo_venta is null then
			v_tipo_venta := -1;
		end if;
	
		v_texto_log := 'v_ind_venta_credito: ' || v_ind_venta_credito;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
		raise notice ' fnc_validar_venta_electronica %', v_texto_log;
	

		if v_ind_venta_credito = true then
			v_respuesta := false;
			raise notice 'Caso 1';
		else
			raise notice 'Caso 2';
			if v_ind_modulo_fe_activo = 'S' then
				raise notice 'Caso 2.1';
				if v_ind_fe_por_defecto = 'S' then
					raise notice 'Caso 2.1.1';
					v_respuesta := true;
				else
					raise notice 'Caso 2.1.2';
					-- Se valida si es consumos propios
					if v_tipo_venta = 3 then
						raise notice 'Caso 2.1.2.1';
						v_respuesta := true;
					else
						raise notice 'Caso 2.1.2.2';
						select coalesce(length((atributos->> 'factura_electronica') ),0) into v_tiene_informacion_fe from ventas v where id = i_id_venta;						
						if v_tiene_informacion_fe > 0 then
							raise notice 'Caso 2.1.2.2.1';
							v_respuesta := true;
						else
							raise notice 'Caso 2.1.2.2.2';
							if v_ind_fe_oblgatorio = 'S' then
								raise notice 'Caso 2.1.2.2.2.1';
								-- Se consulta el valor de la venta
								select importe (s.factor_importe_parcial, total) into v_valor_venta from ventas v join surtidores s on v.surtidor = s.surtidor  where v.id = i_id_venta limit 1;
							
								if v_valor_venta > (v_monto_minimo_fe)::float then
									raise notice 'Caso 2.1.2.2.2.1.1';
									v_respuesta := true;
								else
									raise notice 'Caso 2.1.2.2.2.1.2';
									v_respuesta := false;
								end if; --validación del monto minimo
							else
								raise notice 'Caso 2.1.2.2.2.2';
								v_respuesta := false;								
							end if; --v_ind_fe_oblgatorio
						end if; -- v_tiene_informacion_fe
					end if; -- v_tipo_venta = 3 -- tipo venta consumos propios
				end if; -- v_ind_fe_por_defecto				
			else
				raise notice 'Caso 2.2';
				v_respuesta := false;
			end if; -- v_ind_modulo_fe_activo
		end if; -- v_ind_venta_credito
		
		
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
	           	raise notice 'fnc_validar_venta_electronica %', v_error;
				return false;
	end;
$function$
;