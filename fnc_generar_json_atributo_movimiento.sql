CREATE OR REPLACE FUNCTION public.fnc_generar_json_atributo_movimiento(i_id_venta bigint, i_json_info_consecutivo json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$

	declare 
		v_fecha_hora_inicio					timestamp;
		v_fecha_hora_fin					timestamp;
		v_duracion_proceso					varchar(50) := '';
	
		r_venta								record;
		r_responsable						record;
	
	
		v_id_consumidor_final				bigint := 222222222222;
		
		v_ind_fe_por_defecto				varchar(1) 	:= 'N';
		v_ind_fe_oblgatorio					varchar(1) 	:= 'N';
		v_monto_minimo_fe					varchar(20) := '0';
		
		v_json_movimientos_atributos		text;
		v_personas_nombre					text;
		v_personas_identificacion			text;
		v_tercero_nombre					text;
		v_tercero_identificacion			text;
		v_medio_autorizacion_transaciones 	text;
		v_json_rumbo						text;
	
		v_ind_venta_suspendida				bool  	:= false;
		v_ind_venta_fe						bool 	:= false;
		v_ind_venta_cedito					bool	:= false;
		v_consultar_cliente					bool	:= false;
		v_is_cuenta_local					bool	:= false;
	
	
		v_json_consecutivo					json;
		v_json_extradata					json;
		v_json_precio_diferencial			json;

		v_nombre_up							varchar(100) := 'fnc_generar_json_atributo_movimiento';
		v_texto_log							text;
		v_state   							text;
		v_msg   							text;
		v_detail   							text;
		v_hint   							text;
		v_contex   							text;
		v_error   							text;
	begin 
		v_texto_log := 'Entro. i_id_venta: ' || i_id_venta;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
		raise notice '%', v_texto_log;
	
		v_fecha_hora_inicio := clock_timestamp();	
		---------------------------------------------------------------------------------------------------------------------	
		-- se consulta la información de la venta
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'Se consulta la información de la venta', i_id_venta);
		 select v.operario_id
			  , v.atributos 
			  , coalesce(v.atributos -> 'factura_electronica','{}') 					json_factura_electronica
			  , coalesce(v.atributos -> 'factura_electronica'->> 'nombreComercial','')	personas_nombre
			  , coalesce(v.atributos -> 'factura_electronica'->> 'numeroDocumento','')	personas_identificacion
			  , v.surtidor 
			  , v.cara 
			  , v.manguera 
			  , v.grado
			  , s.islas_id
			  , vd.productos_id 
			  , p.familias 																			id_familia
			  , pf.codigo 																			descripcion_familia
			  , coalesce ((v.atributos->> 'contingencia'),'false')::bool							ind_contigencia
			  , v.id_tipo_venta
			  , case when (v.atributos -> 'DatosFidelizacion') is not null then 'S' else 'N' end 	ind_venta_fidelizada
			  , v.token_process_id 
			  , coalesce ((v.atributos -> 'DatosFactura'->> 'placa'),'')::text 						vehiculo_placa
			  , coalesce ((v.atributos -> 'DatosFactura'->> 'odometro'),'')::text 					vehiculo_odometro
			  , '' 																					vehiculo_numero
			  , importe (s.factor_importe_parcial, v.total) 										valor_total_venta
		  into r_venta
		  from ventas				v
		  join ventas_detalles 		vd 	on v.id = vd.ventas_id 
		  join productos 			p 	on vd.productos_id = p.id 
		  join productos_familias 	pf 	on p.familias = pf.id 
		  join surtidores 			s 	on v.surtidor = s.surtidor
	 left join transacciones		t 	on v.token_process_id = t.id 
		 where v.id 				= i_id_venta
		limit 1
		 ;
		
		v_texto_log := 'r_venta.valor_total_venta' || coalesce (r_venta.valor_total_venta::text , '');
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
		raise notice '% %', v_nombre_up, v_texto_log;
	
		v_texto_log := 'Se consulta la información del promotor';
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
		raise notice '% %', v_nombre_up, v_texto_log;
	
		-- Se consulta la información del promotor 
		select nombres	|| ' ' || apellidos 			responsables_nombre
			 , identificacion  							responsables_identificacion
		  into r_responsable
		  from ct_personas cp 
		 where id = r_venta.operario_id;	
		
	
		if r_venta.json_factura_electronica is not null then 
			v_personas_nombre 			:= r_venta.personas_nombre;
			v_personas_identificacion 	:= r_venta.personas_identificacion;
		else
			-- Se consulta información del consumidor final
			v_texto_log := 'Se consulta información del consumidor final';
			insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
			raise notice '% %', v_nombre_up, v_texto_log;
			
			select nombres	|| ' ' || apellidos 			personas_nombre
				 , identificacion  							personas_identificacion
			  into v_personas_nombre,  v_personas_identificacion
			  from ct_personas cp 
			 where id = v_id_consumidor_final;
		end if;	
		
		v_ind_venta_fe 		:= fnc_validar_venta_electronica(i_id_venta => i_id_venta);
		v_ind_venta_cedito	:= fnc_validar_venta_credito(i_id_venta => i_id_venta)	;
	
		v_texto_log := 'v_ind_venta_fe: ' || v_ind_venta_fe || ' v_ind_venta_cedito: ' || v_ind_venta_cedito;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
		raise notice '% %', v_nombre_up, v_texto_log;
	
	
		v_texto_log := 'r_venta.id_tipo_venta: ' || r_venta.id_tipo_venta;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
		raise notice '% %', v_nombre_up, v_texto_log;
	
		--Si el tipo de venta es cliente propios (10) or rumbo(4)
		if r_venta.id_tipo_venta = 10 or r_venta.id_tipo_venta = 4 then 
			-- Se consulta la trama en transaciones 
			select trama, medio_autorizacion
			  into v_json_extradata, v_medio_autorizacion_transaciones
			  from transacciones t 
			 where t.id = r_venta.token_process_id;	
			-- Se valida transforma data de precio diferencial si viene
			if  (v_json_extradata::json -> 'atributosFlota'->>'precioDiferencial' ) is not null then 
				select 
					row_to_json (t)
				into v_json_precio_diferencial
				from (
						select 
							*
						from (
						select x."familiaId"::int id_familia_h_o, x."valor" valor, x."pos_id"::int id_familia_pos
						from json_to_recordset((v_json_extradata::json ->'atributosFlota'->'precioDiferencial'->'precios')::json ) as x("familiaId"  int, "valor" text, "pos_id" text)
						)t
						where t.id_familia_pos = r_venta.id_familia
				)t;
                  v_json_extradata:= jsonb_set(v_json_extradata::jsonb, '{atributosFlota,precioDiferencial}',to_jsonb(v_json_precio_diferencial) );     
			end if;
			v_texto_log := 'v_json_extradata: ' || coalesce(v_json_extradata::text,'') || ' v_medio_autorizacion_transaciones: ' || coalesce(v_medio_autorizacion_transaciones::text,'') ;
			insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
			raise notice '% %', v_nombre_up, v_texto_log;
		end if;
	
		if r_venta.id_tipo_venta = 4 then  
			r_venta.vehiculo_placa 		:= v_json_extradata::json -> 'response' ->> 'placaVehiculo';
			r_venta.vehiculo_numero 	:= v_json_extradata::json -> 'response' ->> 'vehiculo_numero';
			r_venta.vehiculo_odometro	:= v_json_extradata::json -> 'response' ->> 'vehiculo_odometro';
		
			v_personas_nombre			:= v_json_extradata::json -> 'response' ->> 'nombreCliente';
			v_personas_identificacion	:= v_json_extradata::json -> 'response' ->> 'documentoIdentificacionCliente';	
			
			v_texto_log := 'v_personas_nombre: ' || coalesce(v_personas_nombre::text,'') || ' v_personas_identificacion: ' || coalesce(v_personas_identificacion::text,'') ;
			insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
			raise notice '% %', v_nombre_up, v_texto_log;		
		
			v_json_rumbo := '{"programaCliente":"'	 										|| coalesce((v_json_extradata::json -> 'response' ->> 'programaCliente')::text,'') || '"';
			v_json_rumbo := v_json_rumbo || ',"identificadorTipoDocumentoCliente":"'		|| coalesce((v_json_extradata::json -> 'response' ->> 'identificadorTipoDocumentoCliente')::text,'') || '"';
			v_json_rumbo := v_json_rumbo || ',"nombreCliente":"'	 						|| v_personas_nombre || '"';
			v_json_rumbo := v_json_rumbo || ',"identificadorFormaPago":"'	 				|| coalesce((v_json_extradata::json -> 'response' ->> 'identificadorFormaPago')::text,'') || '"';
			v_json_rumbo := v_json_rumbo || ',"codigoEstacion":"'	 						|| coalesce((v_json_extradata::json -> 'response' ->> 'codigoEstacion')::text,'') || '"';
			v_json_rumbo := v_json_rumbo || ',"placaVehiculo":"'	 						|| coalesce((v_json_extradata::json -> 'response' ->> 'placaVehiculo' )::text,'') || '"';
			v_json_rumbo := v_json_rumbo || ',"identificadorAutorizacion":"'	 			|| coalesce((v_json_extradata::json -> 'response' ->> 'identificadorAprobacion')::text,'') || '"';
			v_json_rumbo := v_json_rumbo || ',"numeroRemisionLazo":"'	 					|| coalesce((v_json_extradata::json -> 'response' ->> 'identificadorAutorizacionEDS')::text,'') || '"';
			v_json_rumbo := v_json_rumbo || ',"numeroTicketeVenta":"'	 					|| coalesce((v_json_extradata::json -> 'response' ->> 'identificadorAutorizacionEDS' )::text,'') || '"';
			v_json_rumbo := v_json_rumbo || ',"medio_autorizacion":"'	 					|| v_medio_autorizacion_transaciones || '"';
			v_json_rumbo := v_json_rumbo || ',"tipoDescuentoCliente":"'	 					|| coalesce((v_json_extradata::json -> 'response' ->> 'tipoDescuentoCliente')::text,'') || '"';
			v_json_rumbo := v_json_rumbo || ',"valorDescuentoCliente":"'	 				||coalesce(( v_json_extradata::json -> 'response' ->> 'valorDescuentoCliente')::text,'') || '"';
			v_json_rumbo := v_json_rumbo || ',"porcentajeDescuentoCliente":"'	 			|| coalesce((v_json_extradata::json -> 'response' ->> 'porcentajeDescuentoCliente')::text,'') || '"}';
		
			v_texto_log := 'v_json_rumbo: ' || coalesce(v_json_rumbo::text,'') ;
			insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
			raise notice '% %', v_nombre_up, v_texto_log;
		
		end if;
		
		if r_venta.id_tipo_venta = 10 then 
			v_is_cuenta_local 			:= true;		
			v_personas_nombre			:= v_json_extradata::json ->> 'nombreCliente';
			v_personas_identificacion	:= coalesce(v_json_extradata::json ->> 'documentoIdentificacionCliente', '');
			r_venta.vehiculo_placa		:= coalesce(v_json_extradata::json ->> 'vehiculo_numero', '');
			r_venta.vehiculo_odometro	:= coalesce(v_json_extradata::json ->> 'vehiculo_odometro', '');
		else
			v_is_cuenta_local := false;
		end if;
	
		if r_venta.id_tipo_venta = -1 then
			r_venta.vehiculo_placa 		:= coalesce(r_venta.atributos::json -> 'DatosFactura' ->> 'placa'::text, '');
			r_venta.vehiculo_numero 	:= coalesce(r_venta.atributos::json -> 'DatosFactura' ->> 'numero_comprobante'::text, '');
			r_venta.vehiculo_odometro	:= coalesce(r_venta.atributos::json -> 'DatosFactura' ->> 'odometro'::text, '');
		end if;
				
		v_ind_fe_por_defecto	:= fnc_consultar_parametro (i_codigo => 'DEFAULT_FE');
		v_ind_fe_oblgatorio		:= fnc_consultar_parametro (i_codigo => 'OBLIGATORIO_FE');
		v_monto_minimo_fe		:= fnc_consultar_parametro (i_codigo => 'MONTO_MINIMO_FE');
	
		v_texto_log := 'v_ind_fe_por_defecto: ' || v_ind_fe_por_defecto || ' v_ind_fe_oblgatorio: ' || v_ind_fe_oblgatorio || ' v_monto_minimo_fe: ' || v_monto_minimo_fe || ' r_venta.valor_total_venta: ' || coalesce (r_venta.valor_total_venta::text , '') ;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
		raise notice '% %', v_nombre_up, v_texto_log;
	
		if v_ind_fe_por_defecto = 'N' 
	 		and ( r_venta.valor_total_venta::float > v_monto_minimo_fe::float) 
	 		and ((v_personas_nombre::text) is not null)
	 		 then 
	 		v_consultar_cliente := true;
		elsif v_ind_fe_por_defecto = 'S' and ((v_personas_nombre::text) is not null) then 
	 		v_consultar_cliente := true;
		end if;
	
		v_texto_log := 'v_consultar_cliente: ' || v_consultar_cliente;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
		raise notice '% %', v_nombre_up, v_texto_log;	
		
		v_json_movimientos_atributos := '{"responsables_nombre":"'	 || r_responsable.responsables_nombre 		|| '"';
		v_json_movimientos_atributos := v_json_movimientos_atributos || ',"responsables_identificacion":"' 		|| r_responsable.responsables_identificacion 			|| '"';
		raise notice '% responsable %', v_nombre_up, v_json_movimientos_atributos;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_movimientos_atributos:' ||  coalesce(v_json_movimientos_atributos::text,''), i_id_venta);
	
		v_json_movimientos_atributos := v_json_movimientos_atributos || ',"personas_nombre":"' 				|| coalesce(v_personas_nombre,'-') 						|| '"';
		v_json_movimientos_atributos := v_json_movimientos_atributos || ',"personas_identificacion":"' 		|| coalesce(v_personas_identificacion,'') 				|| '"';
		raise notice '% personas %', v_nombre_up, v_json_movimientos_atributos;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_movimientos_atributos:' ||  coalesce(v_json_movimientos_atributos::text,''), i_id_venta);
	
		v_json_movimientos_atributos := v_json_movimientos_atributos || ',"tercero_nombre":"' 				|| coalesce(v_tercero_nombre,'') 						|| '"';
		v_json_movimientos_atributos := v_json_movimientos_atributos || ',"tercero_identificacion":"' 		|| coalesce(v_tercero_identificacion ,'')				|| '"';
		raise notice '% tercero %', v_nombre_up, v_json_movimientos_atributos;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_movimientos_atributos:' ||  coalesce(v_json_movimientos_atributos::text,''), i_id_venta);
	
		v_json_movimientos_atributos := v_json_movimientos_atributos || ',"surtidor":'								|| r_venta.surtidor 							|| '';
		v_json_movimientos_atributos := v_json_movimientos_atributos || ',"cara":' 									|| r_venta.cara 								|| '';
		v_json_movimientos_atributos := v_json_movimientos_atributos || ',"manguera":' 								|| r_venta.manguera								|| '';
		v_json_movimientos_atributos := v_json_movimientos_atributos || ',"grado":' 								|| r_venta.grado 								|| '';
		v_json_movimientos_atributos := v_json_movimientos_atributos || ',"islas":"' 								|| r_venta.islas_id 							|| '"';
		raise notice '% venta %', v_nombre_up, v_json_movimientos_atributos;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_movimientos_atributos:' 	||  coalesce(v_json_movimientos_atributos::text,''), i_id_venta);
	
		v_json_movimientos_atributos := v_json_movimientos_atributos || ',"familiaDesc":"' 							|| r_venta.descripcion_familia					|| '"';
		v_json_movimientos_atributos := v_json_movimientos_atributos || ',"familiaId":"' 							|| r_venta.id_familia 							|| '"';
		raise notice '% familia %', v_nombre_up, v_json_movimientos_atributos;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_movimientos_atributos:' 	||  coalesce(v_json_movimientos_atributos::text,''), i_id_venta);
	
	    v_json_movimientos_atributos := v_json_movimientos_atributos || ',"consecutivo":' 							|| coalesce(i_json_info_consecutivo, 'null')	|| '';	   
		raise notice '% conseutivo %', v_nombre_up, v_json_movimientos_atributos;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_movimientos_atributos:' 	||  coalesce(v_json_movimientos_atributos::text,''), i_id_venta);
	   	
	   	v_json_movimientos_atributos := v_json_movimientos_atributos || ',"isElectronica":' 						|| v_ind_venta_fe 								|| '';
	    v_json_movimientos_atributos := v_json_movimientos_atributos || ',"suspendido":' 							|| v_ind_venta_suspendida 						|| '';
		raise notice '% fe - suspendido %', v_nombre_up, v_json_movimientos_atributos;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_movimientos_atributos:' 	||  coalesce(v_json_movimientos_atributos::text,''), i_id_venta);
	   
	   	v_json_movimientos_atributos := v_json_movimientos_atributos || ',"cliente":' 								|| r_venta.json_factura_electronica				|| '';
		raise notice '% cliente %', v_nombre_up, v_json_movimientos_atributos;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_movimientos_atributos:' 	||  coalesce(v_json_movimientos_atributos::text,''), i_id_venta);
	   
	    v_json_movimientos_atributos := v_json_movimientos_atributos || ',"extraData":' 							|| coalesce(v_json_extradata, 'null')			|| '';  
		raise notice '% extradata %', v_nombre_up, v_json_movimientos_atributos; 
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_movimientos_atributos:' 	||  coalesce(v_json_movimientos_atributos::text,''), i_id_venta);
	   
	    v_json_movimientos_atributos := v_json_movimientos_atributos || ',"fidelizada":"' 							|| coalesce(r_venta.ind_venta_fidelizada, 'N')	|| '"';
	    v_json_movimientos_atributos := v_json_movimientos_atributos || ',"vehiculo_placa":"' 						|| coalesce(r_venta.vehiculo_placa::text,'')	|| '"';
	    v_json_movimientos_atributos := v_json_movimientos_atributos || ',"vehiculo_numero":"' 						|| coalesce(r_venta.vehiculo_numero::text,'')	|| '"';
	    v_json_movimientos_atributos := v_json_movimientos_atributos || ',"vehiculo_odometro":"' 					|| coalesce(r_venta.vehiculo_odometro::text,'')	|| '"';  
		raise notice '% fidelizada %', v_nombre_up, v_json_movimientos_atributos; 
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_movimientos_atributos:' 	||  coalesce(v_json_movimientos_atributos::text,''), i_id_venta);
	   
	    v_json_movimientos_atributos := v_json_movimientos_atributos || ',"rumbo":' 								|| coalesce(v_json_rumbo, 'null')			|| '';
	    v_json_movimientos_atributos := v_json_movimientos_atributos || ',"CuentaLocal":' 							|| coalesce(v_json_extradata, 'null')			|| '';
	    v_json_movimientos_atributos := v_json_movimientos_atributos || ',"isCuentaLocal":' 						|| v_is_cuenta_local							|| '';
	    v_json_movimientos_atributos := v_json_movimientos_atributos || ',"identificadorCupo":' 					|| coalesce((v_json_extradata::json ->> 'identificadorCupo')::int,0) || '';  
		raise notice '% rumbo %', v_nombre_up, v_json_movimientos_atributos; 
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_movimientos_atributos:' 	|| coalesce(v_json_movimientos_atributos::text,''), i_id_venta);
	   
	    v_json_movimientos_atributos := v_json_movimientos_atributos || ',"tipoCupo":"' 							|| coalesce((v_json_extradata::json ->> 'tipoCupo')::text,'null') 	|| '"';
	    v_json_movimientos_atributos := v_json_movimientos_atributos || ',"isCredito":"' 							|| v_ind_venta_cedito							|| '"';
	    v_json_movimientos_atributos := v_json_movimientos_atributos || ',"recuperada":' 							|| false										|| '';
	    v_json_movimientos_atributos := v_json_movimientos_atributos || ',"consultarCliente":' 						|| coalesce(v_consultar_cliente, false)			|| '';
	   
	    v_json_movimientos_atributos := v_json_movimientos_atributos || ',"tipoVenta":' 							|| coalesce(r_venta.id_tipo_venta::int,-1)		;	  
		raise notice '% credito %', v_nombre_up, v_json_movimientos_atributos; 
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_movimientos_atributos:' 	||  coalesce(v_json_movimientos_atributos::text,''), i_id_venta);
	
	    v_json_movimientos_atributos := v_json_movimientos_atributos || ',"isContingencia":' 						|| r_venta.ind_contigencia						|| '';  
		raise notice '% contigencia %', v_nombre_up, v_json_movimientos_atributos; 
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_movimientos_atributos:' 	||  coalesce(v_json_movimientos_atributos::text,''), i_id_venta);
	   
		v_json_movimientos_atributos := v_json_movimientos_atributos || '}';
		raise notice '% v_json_movimientos_atributos %', v_nombre_up, v_json_movimientos_atributos;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_movimientos_atributos:' 	||  coalesce(v_json_movimientos_atributos::text,''), i_id_venta);
		---------------------------------------------------------------------------------------------------------------------	
		v_fecha_hora_fin := clock_timestamp();
		v_duracion_proceso	:= age(v_fecha_hora_fin, v_fecha_hora_inicio);
			
		v_texto_log := 'Salio. i_id_venta: ' || i_id_venta || ' Hora Inicio: ' || v_fecha_hora_inicio  || ' Hora Fin: ' || v_fecha_hora_fin  || ' Duración: ' || v_duracion_proceso;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
		raise notice '% v_texto_log %', v_nombre_up, v_texto_log;
		return v_json_movimientos_atributos; 
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
	           return '{""}';
	end;
$function$
;
