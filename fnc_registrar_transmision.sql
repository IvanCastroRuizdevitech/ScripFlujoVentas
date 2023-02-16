CREATE OR REPLACE FUNCTION public.fnc_registrar_transmision(i_id_movimiento bigint)
 RETURNS TABLE(codigo_respuesta character varying, texto_respuesta text, json_response json)
 LANGUAGE plpgsql
AS $function$

	declare 
		v_fecha_hora_inicio				timestamp;
		v_fecha_hora_fin				timestamp;
		v_duracion_proceso				varchar(50) := '';
	
		r_movimientos					record;
	
		v_id_transmimision				bigint;
	
		v_sincronizado					int;
		
		v_json_request					text;
		v_status						text := null;
		v_sql_insert_transmision		text;	
		v_codigo_respuesta				text;
		v_respuesta_insert				text;
	
		v_json_respuesta				json;
		v_json_venta					json;
		v_json_detalle					json;
		v_json_pagos					json;
		v_json_cliente					json;
	
		v_nombre_up						varchar(100) := 'fnc_registrar_transmision';
		v_texto_log						text;
		v_state   						text;
		v_msg   						text;
		v_detail   						text;
		v_hint   						text;
		v_contex   						text;
		v_error   						text;
	begin 
		v_texto_log := 'Entro. i_id_movimiento: ' || i_id_movimiento;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_movimiento);
		raise notice '%', v_texto_log;
	
		v_fecha_hora_inicio := clock_timestamp();	
		---------------------------------------------------------------------------------------------------------------------	
		
		select *
		  into r_movimientos
		  from ct_movimientos cm
		 where cm.id = i_id_movimiento 
		  ;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'Consulta los datos del movimiento', i_id_movimiento);
		
		select (row_to_json(venta)) venta
		  into v_json_venta
		 from (
			select cm.fecha fecha
				 , cm.fecha 	"fechaISO"
				 , cm.consecutivo 
				 , cm.atributos -> 'consecutivo' ->> 'consecutivo_actual' 	"consecutivoActual"
				 , cm.atributos -> 'consecutivo' ->> 'consecutivo_inicial' 	"consecutivoInicial"
				 , cm.atributos -> 'consecutivo' ->> 'consecutivo_final' 	"consecutivoFinal"
				 , cmd.bodegas_id 
				 , cm.empresas_id 
				 , '0' dominios_id
				 , '9' operacion
				 , '12' movimiento_estado
				 , cm.atributos -> 'consecutivo' ->> 'id' 	consecutivo_id
				 , cm.prefijo 
				 , cm.personas_id 
				 , p.identificacion 						persona_nit
				 , p.nombre 								persona_nombre
				 , cm.atributos -> 'personas_nombre' 		persona_nombre
				 , cm.terceros_id 
				 , '' 										tercero_nit
				 , '' 										tercero_nombre
				 , '' 										tercero_responsabilidad_fiscal
				 , ''										tercero_codigo_sap
				 , ''										tercero_correo
				 , ''										tercero_tipo_persona
				 , ''										tercero_tipo_documento
				 , cm.costo_total 
				 , cm.venta_total 
				 , cm.impuesto_total 
				 , cm.descuento_total 
				 , cm.origen_id 
				 , cm.impreso 
				 , cm.remoto_id 
				 , cm.sincronizado 
				 , '0'										sincronizado 
				 , cm.fecha 								create_date
				 , ''										observaciones
				 , ''										"consecutivoReferencia"
		from ct_movimientos cm
		join ct_movimientos_detalles cmd on cm.id = cmd.movimientos_id 
		left join personas p on p.id = cm.personas_id 
		where cm.id = i_id_movimiento
		) venta
		;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_venta: ' || coalesce(v_json_venta::text, ''), i_id_movimiento);
		
		select array_to_json(array_agg((row_to_json(detalle))))detalle
		  into v_json_detalle
		 from (
		select cmd.productos_id				productos_id 
			 , p.descripcion  				producto_descripcion
			 , p.plu						productos_plu
			 , cmd.cantidad
			 , cmd.costo_producto 			costo_unidad
			 , cmd.costo_producto
			 , cmd.precio
			 , cmd.descuentos_id 
			 , cmd.descuento_calculado 
			 , cmd.remoto_id 
			 , cmd.sincronizado 
			 , cmd.sub_total 				subtotal
			 , p.precio 					base
			 , 'N'							compuesto
			 , null							producto_tipo
			 , '[]'::json					ingredientes
			 , '[]'::json					impuestos
			 , false						cortesia
		  from ct_movimientos_detalles 		cmd  
		  join productos					p on p.id = cmd.productos_id 
		where cmd.movimientos_id			= i_id_movimiento
		) detalle;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_detalle: ' || coalesce(v_json_detalle::text, ''), i_id_movimiento);

		select array_to_json(array_agg((row_to_json(pagos))))pagos
		 into v_json_pagos
		 from (
		select cmmp.ct_medios_pagos_id 		medios_pagos_id
			 , cmmp.valor_total 			valor
			 , cmmp.valor_recibido 			recibido
			 , cmmp.valor_cambio 			cambio
		  from ct_movimientos_medios_pagos cmmp
		 where cmmp.ct_movimientos_id  		= i_id_movimiento
		) pagos;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_pagos: ' || coalesce(v_json_pagos::text, ''), i_id_movimiento);
	
		select * 
		  into v_id_transmimision
		  from dblink('pg_remote',' select max(id) + 1 id_transmimision
									  from transmision')
					as consecutivos(id_transmimision bigint);
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_id_transmimision: ' || coalesce(v_id_transmimision::text, ''), i_id_movimiento);
				
		v_json_cliente 	:= '{"mensaje": ""
						  , "fechaProceso": ""
						  , "tipoDocumento":"' 				|| coalesce ((r_movimientos.atributos -> 'cliente' ->> 'tipoDocumento')::text, 'null') 				|| '"
						  , "numeroDocumento":"'			|| coalesce ((r_movimientos.atributos -> 'cliente' ->> 'numeroDocumento')::text, 'null'	)			|| '"
						  , "identificadorTipoPersona":"'	|| coalesce ((r_movimientos.atributos -> 'cliente' ->> 'identificadorTipoPersona')::text, 'null') 	|| '"
						  , "nombreComercial":"'			|| coalesce ((r_movimientos.atributos -> 'cliente' ->> 'nombreComercial')::text, '' )				|| '"
						  , "nombreRazonSocial":"'			|| coalesce ((r_movimientos.atributos -> 'cliente' ->> 'nombreRazonSocial')::text, '' )				|| '"
						  , "direccion":"'					|| coalesce ((r_movimientos.atributos -> 'cliente' ->> 'direccion')::text, '' 	)					|| '"
						  , "correoElectronico":"'			|| coalesce ((r_movimientos.atributos -> 'cliente' ->> 'correoElectronico')::text, '' )				|| '"
						  , "departamento":"'				|| coalesce ((r_movimientos.atributos -> 'cliente' ->> 'departamento')::text, '' )					|| '"
						  , "regimenFiscal":"'				|| coalesce ((r_movimientos.atributos -> 'cliente' ->> 'regimenFiscal')::text, '' )					|| '"				
						  , "telefono":"'					|| coalesce ((r_movimientos.atributos -> 'cliente' ->> 'telefono')::text, '') 						|| '"
						  , "tipoResponsabilidad":"'		|| coalesce ((r_movimientos.atributos -> 'cliente' ->> 'tipoResponsabilidad')::text, '') 			|| '"
						  , "codigoSAP":"'					|| coalesce ((r_movimientos.atributos -> 'cliente' ->> 'codigoSAP')::text, '' )						|| '"
						  , "extraData":'					|| coalesce ((r_movimientos.atributos -> 'cliente' ->> 'extraData')::text, 'null') 					|| '
						  , "direccionFiscalCliente":'		|| coalesce ((r_movimientos.atributos -> 'cliente' ->> 'direccionFiscalCliente')::text, 'null') 	|| '
						  , "tipoResponsabilidad":"'		|| coalesce ((r_movimientos.atributos -> 'cliente' ->> 'tipoResponsabilidad')::text, 'null' )		|| '"
						  , "datosTributariosAdquirente":"'	|| coalesce ((r_movimientos.atributos -> 'cliente' ->> 'datosTributariosAdquirente')::text, 'null') || '"
						  , "ventaIds":""
						}';	
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, '------- r_movimientos.atributos: ' || coalesce((r_movimientos.atributos -> 'cliente')::text, ''), i_id_movimiento);
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, '------- r_movimientos.atributos: ' || length(coalesce((r_movimientos.atributos -> 'cliente')::text, '')), i_id_movimiento);

		if length(coalesce((r_movimientos.atributos -> 'cliente')::text, '')) > 2 then
			v_sincronizado 	:= 2;
		else 
			v_sincronizado 	:= 3;
		end if;  
		
		raise notice 'v_sincronizado %',  coalesce(v_sincronizado::text,'');	   
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_sincronizado:' ||  coalesce(v_sincronizado::text,''), i_id_movimiento);
	
		v_json_request	:= '{';
		v_json_request	:= v_json_request || '"venta":' 	|| v_json_venta;
		v_json_request	:= v_json_request || ',"detalle":'	|| v_json_detalle;
		v_json_request	:= v_json_request || ',"pagos":'	|| v_json_pagos;
		v_json_request	:= v_json_request || ',"test": false, "retener":false, "identificadorMovimiento": ' || i_id_movimiento || ',"id_transmision":' || v_id_transmimision ;
		v_json_request	:= v_json_request || ',"cliente":'	|| coalesce(v_json_cliente::text,'null');
		--v_json_request	:= v_json_request || ',"origen":"SR"';
		v_json_request	:= v_json_request || '}';
	
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_request: ' || coalesce(v_json_request::text, ''), i_id_movimiento);
	
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_request:' ||  coalesce(v_json_request::text,''), i_id_movimiento);
		raise notice 'v_json_request %',  coalesce(v_json_request::text,'');
	    
		raise notice 'v_id_transmimision %',  coalesce(v_id_transmimision::text,'');	
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_id_transmimision:' ||  coalesce(v_id_transmimision::text,''), i_id_movimiento);    
		
		raise notice 'r_movimientos.equipos_id %',  coalesce(r_movimientos.equipos_id::text,'');	
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'r_movimientos.equipos_id:' ||  coalesce(r_movimientos.equipos_id::text,''), i_id_movimiento);  
		
		raise notice 'v_status %',  coalesce(v_status::text,'');	  
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_status:' ||  coalesce(v_status::text,''), i_id_movimiento);
	
		v_sql_insert_transmision := 'insert into transmision (id, equipo_id, request, sincronizado, fecha_generado, status, url, method, reintentos) values (' || v_id_transmimision || ', ' || r_movimientos.equipos_id || ', ''' || v_json_request || ''', ' || v_sincronizado || ', ''' || clock_timestamp() || ''' , ' || 'null' || ', '''', '''', 0);';
		
		raise notice 'v_sql_insert_transmision %',  coalesce(v_sql_insert_transmision::text,'');
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_sql_insert_transmision:' ||  coalesce(v_sql_insert_transmision::text,''), i_id_movimiento);
	
		if v_sql_insert_transmision is not null then
			select * 
		      into v_respuesta_insert
		      from dblink_exec('pg_remote', v_sql_insert_transmision ,false);	
		end if;
		
	    raise notice 'v_respuesta_insert: %', v_respuesta_insert;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_respuesta_insert:' ||  coalesce(v_respuesta_insert::text,''), i_id_movimiento);
		
	    codigo_respuesta	:= 'OK';
		--texto_respuesta		:= 'OK';
		texto_respuesta		:= 'Se registro en transmision id: ' || coalesce(v_id_transmimision::text,'');
		json_response		:= v_json_request::json;
						
		v_texto_log := 'codigo_respuesta: ' || codigo_respuesta || ' texto_respuesta: ' || texto_respuesta || ' json_response: ' || coalesce(json_response::text,'') ;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_movimiento);
		---------------------------------------------------------------------------------------------------------------------	
		v_fecha_hora_fin := clock_timestamp();
		v_duracion_proceso	:= age(v_fecha_hora_fin, v_fecha_hora_inicio);
			
		v_texto_log := 'Salio. i_id_movimiento: ' || i_id_movimiento || ' Hora Inicio: ' || v_fecha_hora_inicio  || ' Hora Fin: ' || v_fecha_hora_fin  || ' Duración: ' || v_duracion_proceso;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_movimiento);
		return next; 
	exception
		when others then
			get STACKED diagnostics  
	        	v_state   = RETURNED_SQLSTATE,
				v_msg     = MESSAGE_TEXT,
				v_detail  = PG_EXCEPTION_DETAIL,
				v_hint    = PG_EXCEPTION_HINT;
				v_error	:= v_state || ' ' || v_msg || ' ' || v_detail || ' ' || v_hint;
	           	insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_error, i_id_movimiento);
	           	raise notice '%', v_error;
			    codigo_respuesta	:= 'ERROR';
				texto_respuesta		:= v_error;
				json_response		:= null;
	           	return next;
	end;
$function$
;
