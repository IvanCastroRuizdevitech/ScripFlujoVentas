CREATE OR REPLACE FUNCTION public.fnc_generar_consecutivo_movimiento(i_id_venta bigint, i_ind_contigencia boolean, i_id_equipo bigint, i_id_producto bigint)
 RETURNS TABLE(codigo_respuesta character varying, texto_respuesta text, id_consecutivo integer, consecutivo integer, prefijo character varying, consecutivo_completo character varying, json_info_consecutivo json, consecutivo_lazoexpressregistry boolean)
 LANGUAGE plpgsql
AS $function$

	declare
		v_fecha_hora_inicio				timestamp;
		v_fecha_hora_fin				timestamp;
		v_duracion_proceso				varchar(50) := '';
	
		r_consecutivos					record;
	
		v_tipo_documento_register		int 		:= 9;
		v_tipo_venta					int;
		
		v_tipo_documento				varchar(10);
		v_destino						varchar(10);
		v_tipo_documento_core			varchar(10)	:= '018';
		v_facturacion_externa			varchar(1);
		v_indicador_remision			varchar(1) 	:= 'N';
	
		v_select_consecutivo			text;
	
		v_consulta_lazoexpressregistry	bool		:= false;
		v_consulta_x_producto			bool		:= false; -- Indicador si el consecutivo se va consultar por producto		
		v_ind_fe						bool		:= false;
		v_consulta_consecutivo			bool		:= false;
		v_ind_venta_credito				bool 		:= false;
	
		v_nombre_up						varchar(100) := 'fnc_generar_consecutivo_movimiento';
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
		raise notice 'fnc_generar_consecutivo_movimiento %', v_texto_log;
	
		v_fecha_hora_inicio := clock_timestamp();	
		---------------------------------------------------------------------------------------------------------------------	
	
		v_indicador_remision	:= fnc_consultar_parametro (i_codigo => 'REMISION');
		v_facturacion_externa	:= fnc_consultar_parametro (i_codigo => 'FACTURACION_EXTERNA');
		v_ind_fe 				:= fnc_validar_venta_electronica (i_id_venta => i_id_venta);
		v_ind_venta_credito		:= fnc_validar_venta_credito(i_id_venta => i_id_venta);
			
		v_texto_log := 'v_ind_fe: ' || v_ind_fe || ' i_ind_contigencia: ' || i_ind_contigencia;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
		raise notice 'fnc_generar_consecutivo_movimiento %', v_texto_log;
	
		-- Se consulta el tipo de venta
		select v.id_tipo_venta 
		  into v_tipo_venta
		  from ventas v 
		 where id = i_id_venta;
		
		/*if v_indicador_remision = 'S' or v_ind_venta_credito is true then -- validar calibraciones / consumo propio
			v_texto_log						:= 'Caso 0';
			v_consulta_consecutivo			:= false;*/
		if v_tipo_venta = 2 or v_tipo_venta = 10 or v_tipo_venta = 3 or v_tipo_venta = 100 or v_tipo_venta = 4 then			
			v_texto_log						:= 'Caso 0.1';
			v_consulta_consecutivo			:= false;
		else
			-- 1. 
			if v_ind_fe = true and i_ind_contigencia = true then
				v_texto_log						:= 'Caso 1';
				v_consulta_consecutivo			:= true;
				v_tipo_documento 				:= '018';
				v_destino						:= 'COM';
				v_consulta_lazoexpressregistry	:= false;
				v_consulta_x_producto			:= true;
			
			elsif v_ind_fe = true and i_ind_contigencia = false then 
				v_texto_log	:= 'Caso 2';
				if v_facturacion_externa = 'S' then
					v_texto_log						:= 'Caso 2.1';
					v_consulta_consecutivo			:= true;
					v_tipo_documento 				:= '31';
					v_destino						:= 'CAN';
					v_consulta_lazoexpressregistry	:= true;
					v_consulta_x_producto			:= false;
				else
					v_texto_log						:= 'Caso 2.2';
					v_consulta_consecutivo			:= true;
					v_tipo_documento 				:= '031';
					v_destino						:= 'COM';
					v_consulta_lazoexpressregistry	:= false;
					v_consulta_x_producto			:= false;
				end if;
				
			elsif v_ind_fe = false and i_ind_contigencia = true then
				v_texto_log							:= 'Caso 3';
				v_consulta_consecutivo				:= true;
				v_tipo_documento 					:= '018';
				v_destino							:= 'COM';
				v_consulta_lazoexpressregistry		:= false;
				v_consulta_x_producto				:= true;
			elsif v_ind_fe = false and i_ind_contigencia = false then
				v_texto_log	:= 'Caso 4';
				if v_facturacion_externa = 'S' then
					v_texto_log						:= 'Caso 4.1';
					v_consulta_consecutivo			:= true;
					v_tipo_documento 				:= '9';
					v_consulta_lazoexpressregistry	:= true;
					v_consulta_x_producto			:= true;
				else
					v_texto_log						:= 'Caso 4.2';
					v_consulta_consecutivo			:= true;
					v_tipo_documento 				:= '009';
					v_consulta_lazoexpressregistry	:= false;
					v_consulta_x_producto			:= true;
			
				end if;
			end if;
		end if;
	
		raise notice ' % >>>> %', v_nombre_up, v_texto_log;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);

		if v_consulta_consecutivo then	
			v_texto_log := '--- i_id_equipo: ' || i_id_equipo || ' v_tipo_documento: ' || v_tipo_documento || ' v_destino: ' || coalesce(v_destino,'') || ' v_consulta_lazoexpressregistry: ' || v_consulta_lazoexpressregistry || ' v_consulta_x_producto: ' || v_consulta_x_producto || ' i_id_producto: ' || i_id_producto;
			raise notice ' >>>> %', v_texto_log;
			insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
		
			-- Se consulta el consecutivo
			begin
				if v_consulta_lazoexpressregistry then
					v_texto_log	:= 'Consulta 1';
					raise notice ' >>>> v_texto_log %', v_texto_log;
					insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
					
					if v_consulta_x_producto then
						v_texto_log	:= 'Consulta 1.1';
						raise notice ' >>>> v_texto_log %', v_texto_log;
						insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
						v_select_consecutivo := ' select cc.id
														 , coalesce(cc.consecutivo_actual,0) consecutivo_actual
														 , cc.prefijo
														 , cc.fecha_inicio
														 , cc.fecha_fin
														 , cc.consecutivo_inicial
														 , cc.consecutivo_final
														 , row_to_json (cc.*) json_info_consecutivo
													  from consecutivos											cc 
													 where cc.estado 											!= ''I''
													   and cc.tipo_documento									= '		|| v_tipo_documento::int 	|| '
													   and cc.equipos_id										= ' 	|| i_id_equipo 				|| '
													   and case when length(''' || coalesce( v_destino, '') || ''')> 0 then (cc.cs_atributos::json->>''destino'')::text =  ''' || coalesce( v_destino, '') || ''' else 1 = 1 end
													   and (cc.cs_atributos::json->''producto_id'')::text 		= ''' 	|| i_id_producto 			|| '''
												  order by cc.fecha_fin asc
													 limit 1';
						insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_select_consecutivo, i_id_venta);
						raise notice ' >>>> 1.1 v_select_consecutivo %', v_select_consecutivo;
						
						begin 
							select * 
							  into r_consecutivos
							  from dblink('pg_remote',v_select_consecutivo)
										as consecutivos(id_consecutivo int, consecutivo_actual int, prefijo varchar(10), fecha_inicio timestamp, fecha_fin timestamp, consecutivo_inicial bigint, consecutivo_final bigint, json_info_consecutivo json);
						exception
							when others then
								get STACKED diagnostics  
									v_state   = RETURNED_SQLSTATE,
									v_msg     = MESSAGE_TEXT,
									v_detail  = PG_EXCEPTION_DETAIL,
									v_hint    = PG_EXCEPTION_HINT;
									v_error	:= 'Error al consultar la información del consecutivo Consulta 1.1 ' ||  v_state || ' ' || v_msg || ' ' || v_detail || ' ' || v_hint;
									insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_error, i_id_venta);
									codigo_respuesta 	:= 'ERROR';
									texto_respuesta		:= v_error;
									raise notice '%', v_error;
						end; -- Fin select Consecutivo Consulta 1.1
						
						raise notice ' >>>> r_consecutivos.id_consecutivo %', coalesce(r_consecutivos.id_consecutivo,-1);
						if r_consecutivos.id_consecutivo is null then
							v_texto_log	:= 'Consulta 1.1.1';
							raise notice ' >>>> v_texto_log %', v_texto_log;
							insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
						 	
							v_select_consecutivo := ' select cc.id
															 , coalesce(cc.consecutivo_actual,0) consecutivo_actual
															 , cc.prefijo
															 , cc.fecha_inicio
															 , cc.fecha_fin
															 , cc.consecutivo_inicial
															 , cc.consecutivo_final
															 , row_to_json (cc.*) json_info_consecutivo
														  from consecutivos											cc 
														 where cc.estado 											!= ''I''
														   and cc.tipo_documento									= '		|| v_tipo_documento::int 	|| '
														   and cc.equipos_id										= ' 	|| i_id_equipo 				|| '
														   and case when length(''' || coalesce(v_destino, '') || ''')> 0 then (cc.cs_atributos::json->>''destino'')::text =  ''' || coalesce(v_destino, '') || ''' else 1 = 1 end
														   and (cc.cs_atributos::json->''producto_id'')::text 		is null
													  order by cc.fecha_fin asc
														 limit 1';
							insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_select_consecutivo, i_id_venta);
							raise notice ' Consulta 1.1.1 v_select_consecutivo - %', v_select_consecutivo;
						
							begin 
								select * 
								  into r_consecutivos
								  from dblink('pg_remote',v_select_consecutivo)
											as consecutivos(id_consecutivo int, consecutivo_actual int, prefijo varchar(10), fecha_inicio timestamp, fecha_fin timestamp, consecutivo_inicial bigint, consecutivo_final bigint, json_info_consecutivo json);
							exception
								when others then
									get STACKED diagnostics  
										v_state   = RETURNED_SQLSTATE,
										v_msg     = MESSAGE_TEXT,
										v_detail  = PG_EXCEPTION_DETAIL,
										v_hint    = PG_EXCEPTION_HINT;
										v_error	:= 'Error al consultar la información del consecutivo Consulta 1.1.1 ' ||  v_state || ' ' || v_msg || ' ' || v_detail || ' ' || v_hint;
										insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_error, i_id_venta);
										codigo_respuesta 	:= 'ERROR';
										texto_respuesta		:= v_error;
										raise notice '%', v_error;
							end; -- Fin select Consecutivo Consulta 1.1.1
						 end if; --  FIN r_consecutivos.id_consecutivo is null
					else -- v_consulta_x_producto = FLASE
						v_texto_log	:= 'Consulta 1.2';
						raise notice ' >>>> v_texto_log %', v_texto_log;
						insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
					
						v_select_consecutivo := 'select cc.id
													  , coalesce(consecutivo_actual,0) consecutivo_actual
													  , cc.prefijo
													  , cc.fecha_inicio
													  , cc.fecha_fin
													  , cc.consecutivo_inicial
													  , cc.consecutivo_final													 
													  , row_to_json (cc.*) json_info_consecutivo
												   from consecutivos										cc 
												  where cc.estado 											!= ''I''
												    and cc.tipo_documento									= ' 	|| v_tipo_documento::int	|| '
												    and cc.equipos_id										= ' 	|| i_id_equipo 				|| '
													and case when length(''' || coalesce(v_destino, '') || ''')> 0 then (cc.cs_atributos::json->>''destino'')::text =  ''' || coalesce(v_destino, '') || ''' else 1 = 1 end
												    and (cc.cs_atributos::json->''producto_id'')::text		is null
											   order by cc.fecha_fin asc
												  limit 1';
						insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_select_consecutivo, i_id_venta);
						raise notice ' Consulta 1.2 v_select_consecutivo - %', v_select_consecutivo;
					
						begin 
							select * 
							  into r_consecutivos
							  from dblink('pg_remote',v_select_consecutivo)
										as consecutivos(id_consecutivo int, consecutivo_actual int, prefijo varchar(10), fecha_inicio timestamp, fecha_fin timestamp, consecutivo_inicial bigint, consecutivo_final bigint, json_info_consecutivo json);
						exception
							when others then
								get STACKED diagnostics  
									v_state   = RETURNED_SQLSTATE,
									v_msg     = MESSAGE_TEXT,
									v_detail  = PG_EXCEPTION_DETAIL,
									v_hint    = PG_EXCEPTION_HINT;
									v_error	:= 'Error al consultar la información del consecutivo Consulta 1.2 ' ||  v_state || ' ' || v_msg || ' ' || v_detail || ' ' || v_hint;
									insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_error, i_id_venta);
									codigo_respuesta 	:= 'ERROR';
									texto_respuesta		:= v_error;
									raise notice '%', v_error;
						end; -- Fin select Consecutivo Consulta 1.2
					end if; --v_consulta_x_producto
				else -- v_consulta_lazoexpressregistry = false
					v_texto_log	:= 'Consulta 2';
					raise notice ' ----- v_texto_log %', v_texto_log;
					insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
					
					if v_consulta_x_producto then
						v_texto_log	:= 'Consulta 2.1';
						raise notice ' ----- v_texto_log %', v_texto_log;
						insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
						
						select cc.id								id_consecutivo
							 , coalesce(cc.consecutivo_actual,0) 	consecutivo_actual
							 , cc.prefijo
							 , cc.fecha_inicio
							 , cc.fecha_fin
							 , cc.consecutivo_inicial
							 , cc.consecutivo_final 
							 , row_to_json (cc.*) 					json_info_consecutivo
						  into r_consecutivos
						  from ct_consecutivos cc 
						 where cc.estado 									!= 'I' 
						   and cc.tipo_documento							= v_tipo_documento  
						   and cc.equipos_id								= i_id_equipo
						   and coalesce((cc.cs_atributos::json->'producto_id')::text, '99')::int = i_id_producto
					  order by cc.fecha_fin asc
						 limit 1;
						
						if r_consecutivos.id_consecutivo is null then
							v_texto_log	:= 'Consulta 2.1.1';
							raise notice ' ----- v_texto_log %', v_texto_log;
							select cc.id								id_consecutivo
								 , coalesce(cc.consecutivo_actual,0) 	consecutivo_actual
								 , cc.prefijo
								 , cc.fecha_inicio
								 , cc.fecha_fin
								 , cc.consecutivo_inicial
								 , cc.consecutivo_final
								 , row_to_json (cc.*) 					json_info_consecutivo
							  into r_consecutivos
							  from ct_consecutivos 							cc
							 where cc.estado 								!= 'I'
							   and cc.tipo_documento 						= v_tipo_documento
							   and cc.equipos_id							= i_id_equipo
							   and (cc.cs_atributos::json->>'destino') 		= v_destino
							   and (cc.cs_atributos::json->'producto_id')	is null
						  order by fecha_fin asc
							 limit 1;
						 end if;
					else -- v_consulta_x_producto = false
						v_texto_log	:= 'Consulta 2.2';
						raise notice ' ----- v_texto_log %', v_texto_log;
					
						select cc.id									id_consecutivo
							 , coalesce(consecutivo_actual,0) 			consecutivo_actual
							 , cc.prefijo
							 , cc.fecha_inicio
							 , cc.fecha_fin
							 , cc.consecutivo_inicial
							 , cc.consecutivo_final
							 , row_to_json (cc.*) 						json_info_consecutivo
						  into r_consecutivos
						  from ct_consecutivos 							cc
						 where cc.estado 								!= 'I'
						   and cc.tipo_documento 						= v_tipo_documento
						   and cc.equipos_id							= i_id_equipo
						   and (cc.cs_atributos::json->>'destino') 		= v_destino
						   and (cc.cs_atributos::json->'producto_id')	is null
					  order by fecha_fin asc
						 limit 1;
					end if;
				end if; --v_consulta_lazoexpressregistry = true
										
				v_texto_log := 'Datos del consecutivo consultado ' 			||
							   ' r_consecutivos.id_consecutivo: ' 			|| r_consecutivos.id_consecutivo 		|| 
							   ' r_consecutivos.consecutivo_actual: ' 		|| r_consecutivos.consecutivo_actual 	|| 
							   ' r_consecutivos.consecutivo_inicial: ' 		|| r_consecutivos.consecutivo_inicial	|| 
							   ' r_consecutivos.consecutivo_final: ' 		|| r_consecutivos.consecutivo_final		|| 
							   ' r_consecutivos.fecha_inicio: ' 			|| r_consecutivos.fecha_inicio			|| 
							   ' r_consecutivos.fecha_fin: ' 				|| r_consecutivos.fecha_fin				|| 
							   ' r_consecutivos.consecutivo_final: ' 		|| r_consecutivos.prefijo				||
							   ' r_consecutivos.json_info_consecutivo: '	|| r_consecutivos.json_info_consecutivo ||
							   ' v_consulta_lazoexpressregistry: '			|| v_consulta_lazoexpressregistry;
				raise notice ' >>>>>>>  %', v_texto_log;
				insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
				
				if r_consecutivos.id_consecutivo is null then
					codigo_respuesta 				:= 'ERROR';
					texto_respuesta					:= 'No se encontro una resolución';				
				elsif r_consecutivos.consecutivo_actual < r_consecutivos.consecutivo_inicial  then
					codigo_respuesta 				:= 'ERROR';
					texto_respuesta					:= 'El consecutivo actual es menor al consecutivo inicial';
				elsif r_consecutivos.consecutivo_actual > r_consecutivos.consecutivo_final then
					codigo_respuesta 				:= 'ERROR';
					texto_respuesta					:= 'El consecutivo actual es mayor al consecutivo final';
				elsif r_consecutivos.fecha_inicio > clock_timestamp() or r_consecutivos.fecha_fin <  clock_timestamp() then
					codigo_respuesta 				:= 'ERROR';
					texto_respuesta					:= 'La resolución no se encuentra vigente';
				elsif r_consecutivos.id_consecutivo is not null then
					codigo_respuesta 				:= 'OK';
					texto_respuesta					:= 'Generación del consecutivo exitoso';
					id_consecutivo					:= r_consecutivos.id_consecutivo;
					consecutivo 					:= r_consecutivos.consecutivo_actual;
					prefijo  						:= r_consecutivos.prefijo;
					consecutivo_completo  			:= r_consecutivos.prefijo || ' - ' || r_consecutivos.consecutivo_actual;
					json_info_consecutivo  			:= r_consecutivos.json_info_consecutivo;
					consecutivo_lazoexpressregistry	:= v_consulta_lazoexpressregistry;
				end if;
				
				raise notice '% :::::::: --- :::::::: % %', v_nombre_up, codigo_respuesta, texto_respuesta;
				insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, codigo_respuesta || ' ' || texto_respuesta, i_id_venta);
				return next;
					
			exception
				when others then
					get STACKED diagnostics  
						v_state   = RETURNED_SQLSTATE,
						v_msg     = MESSAGE_TEXT,
						v_detail  = PG_EXCEPTION_DETAIL,
						v_hint    = PG_EXCEPTION_HINT;
						v_error	:= 'Error al consultar la información del consecutivo ' ||  v_state || ' ' || v_msg || ' ' || v_detail || ' ' || v_hint;
						insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_error, i_id_venta);
						codigo_respuesta 	:= 'ERROR';
						texto_respuesta		:= v_error;
						raise notice '%', v_error;
			end;
		 
		else
			codigo_respuesta 				:= 'OK';
			texto_respuesta					:= 'Generación del consecutivo exitoso';
			id_consecutivo					:= null;
			consecutivo 					:= i_id_venta;
			prefijo  						:= null;
			consecutivo_completo  			:= i_id_venta;
			json_info_consecutivo  			:= null;
			consecutivo_lazoexpressregistry	:= false;
			
			raise notice '% :::::::: --- :::::::: % %', v_nombre_up, codigo_respuesta, texto_respuesta;
			insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, codigo_respuesta || ' ' || texto_respuesta, i_id_venta);
			return next;
		end if; -- v_consulta_consecutivo
		
		---------------------------------------------------------------------------------------------------------------------	
		v_fecha_hora_fin := clock_timestamp();
		v_duracion_proceso	:= age(v_fecha_hora_fin, v_fecha_hora_inicio);
			
		v_texto_log := 'Salio. i_id_venta: ' || i_id_venta || ' Hora Inicio: ' || v_fecha_hora_inicio  || ' Hora Fin: ' || v_fecha_hora_fin  || ' Duración: ' || v_duracion_proceso;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
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
	end;
$function$
;
