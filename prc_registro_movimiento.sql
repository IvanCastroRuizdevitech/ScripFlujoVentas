CREATE OR REPLACE PROCEDURE public.prc_registro_movimiento(i_id_venta bigint, INOUT o_json_respuesta json)
 LANGUAGE plpgsql
AS $procedure$

	declare
		v_fecha_hora_inicio							timestamp;
		v_fecha_hora_fin							timestamp;
		v_duracion_proceso							varchar(50) := '';
				
		r_venta										record;
		r_venta_detalle								record;
		r_equipos									record;
		r_productos									record;
		r_venta_medio_pagos							record;
		r_empresa									record;
		c_ventas_detalle							record;
		c_ventas_medios_pago						record;
		c_precios_diferenciales						record;
	
		v_id_consecutivo							int;
		v_consecutivo_actual						int;
		v_ano										int;
		v_mes										int;
		v_dia										int;
		v_origen_id									int;
		v_count_insert_movimiento_detalle			int 		:= 0;
		v_count_insert_movimiento_medios_pago		int 		:= 0;
		v_count_venta_movimiento					int			:= 0;
		v_id_medio_pago_atributos					int			:= 0;
		v_total_valor_venta_precio_diferencial		int 		:= 0;
		
		v_id_movimiento								bigint;
		v_id_movimiento_detalle						bigint;
		v_id_persona								bigint		:= 2;
		v_id_bodega									bigint;
		v_id_medio_pago_x_defecto					bigint;
				
		v_total_medio_pago							float;
		v_valor_total_venta							float;	
		v_sub_total									float;
	
		v_codigo_respuesta_consecutivo 				varchar(10);
		v_prefijo 									varchar(20);
		v_consecutivo_completo						varchar(50);
		v_tipo_movimiento							varchar(5)	:= '017';
		v_estado_movimiento							varchar(10)	:= '017000';
		v_impreso									varchar(1)	:= 'N';
		v_ind_modulo_fe_activo						varchar(1) 	:= 'N'; -- indicador de que si el modulo de factuación electronica esta activo
		v_ind_fe_por_defecto						varchar(1) 	:= 'N';
		v_ind_fe_oblgatorio							varchar(1) 	:= 'N';
		v_monto_minimo_fe							varchar(20) := '0';
		v_descripcion_medio_pago					varchar(20) := 'EFECTIVO';
		
		v_texto_respuesta_consecutivo				text;
		v_respuesta_update							text;
		v_respuesta_notify							text;
		v_empresa_dococumento_alias					text;
		v_json_data_detalle							text		:= '';
		v_json_data_medio_pago						text		:= '';
		v_parametros_host_principal					text		:= '';
		v_json_atributos_venta						text;
		v_codigo_respuesta_registro_trans			text;
		v_texto_respuesta_registro_trans			text;
		v_json_response								text;
		
		v_json_consecutivo							json;
		v_json_atributos							json;
		v_json_atributos_detalle					json;
		v_json_data									json		:= '{}';
		
		v_ind_contngencia							bool 		:= false;
		v_consecutivo_lazoexpressregistry			bool		:= false;
		v_ind_pos_principal							bool		:= false;	
		v_ind_venta_fe								bool		:= false;
		
		------------------------------------------------------------------------------------------------------------------------
		v_nombre_up									varchar(100) := 'prc_registro_movimiento';
		v_json_respuesta							json;
		v_codigo_respuesta							varchar(10);
		v_texto_log									text;
		v_state   									text;
		v_msg   									text;
		v_detail   									text;
		v_hint   									text;
		v_contex   									text;
		v_error   									text;
	
	begin 
		--truncate table logs;
		v_texto_log := 'Entro. i_id_venta: ' || i_id_venta;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
		raise notice 'prc_registro_movimiento %', v_texto_log;
	
		v_fecha_hora_inicio := clock_timestamp();
		
		---------------------------------------------------------------------------------------------------------------------
		
		v_parametros_host_principal		:= fnc_consultar_parametro (i_codigo => 'HOST_LAZO_PRINCIPAL') ;
		v_ind_modulo_fe_activo			:= fnc_consultar_parametro (i_codigo => 'MODULO_FACTURACION_ELECTRONICA');
		v_ind_fe_por_defecto			:= fnc_consultar_parametro (i_codigo => 'DEFAULT_FE');
		v_ind_fe_oblgatorio				:= fnc_consultar_parametro (i_codigo => 'OBLIGATORIO_FE');
		v_monto_minimo_fe				:= fnc_consultar_parametro (i_codigo => 'MONTO_MINIMO_FE');
		
		if v_parametros_host_principal = 'localhost' then
			v_ind_pos_principal := true;
		else
			v_ind_pos_principal := false;
		end if;
	
		-- Se consulta la información de la venta
		select v.*
			 , importe (s.factor_importe_parcial, v.total) 					valor_total_venta
			 , (v.atributos -> 'DatosFactura'->> 'medio_pago')::int 		id_medio_pago_atributos
			 , (v.atributos -> 'DatosFidelizacion') 						datos_fidelizacion
			 , (t.trama::json -> 'atributosFlota'->>'precioDiferencial' )	datos_precio_diferencial 
		  into r_venta
		  from ventas 			v
		  join surtidores 		s on v.surtidor = s.surtidor
	  left join transacciones t on t.id = v.token_process_id 
		 where v.id 			= i_id_venta
		 limit 1;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'Consulto la venta', i_id_venta);
		v_id_medio_pago_atributos := coalesce(r_venta.id_medio_pago_atributos::int, null);
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, '--------------------- Se consulta la información de la venta - r_venta.datos_fidelizacion ' || coalesce(r_venta.datos_fidelizacion::text, '') , i_id_venta);
	
		-- Se consulta el detalle de la venta 
		select vd.*
		  into r_venta_detalle
		  from ventas_detalles 	vd
		 where vd.ventas_id		= i_id_venta
	  order by vd.id
	     limit 1;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'Consulto el detalle de la venta', i_id_venta);
	   
	    -- se consultan los medios de pago de la venta
	    select count(id) cantidad_medios_pago, sum(valor_total) valor_total_medios_pago
	     into r_venta_medio_pagos
	      from ventas_medios_pagos vmp 
	     where vmp.ventas_id =  i_id_venta;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'Consulto los medios de pago de la venta', i_id_venta);
		
		-- Se consulta la información del equipo
		select e.id, e.empresas_id
		  into r_equipos 
		  from equipos e 
		 limit 1;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'Consulto la info de equipo', i_id_venta);
		 
		-- Se consulta la información de la empresa
		select e.razon_social, e.nit, e.direccion, e.telefono, e.ciudades_descripcion, e.alias, d.header, d.footer
		  into r_empresa
		  from empresas		e
		  join descriptores	d on e.id = d.empresas_id
		 where e.id = r_equipos.empresas_id;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'Consulto la info de la empresa', i_id_venta);
	
		-- Se consulta la información del tipo de venta					
	 	select tv.id_tipo_medio_pago_defecto, tv.tipo_movimiento, tv.estado_movimiento
	 	  into v_id_medio_pago_x_defecto, v_tipo_movimiento, v_estado_movimiento
	 	  from tipos_venta  tv
	 	 where tv.id_tipo_venta = r_venta.id_tipo_venta;
	 	
		v_texto_log := 'v_id_medio_pago_x_defecto: ' || v_id_medio_pago_x_defecto || ' v_tipo_movimiento: '  || v_estado_movimiento || ' v_tipo_movimiento: '  || v_estado_movimiento;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
		raise notice '%  %', v_nombre_up, v_texto_log;
			 	
		-- Se consulta si la venta es de constingencia
		v_ind_contngencia := coalesce ((r_venta.atributos->> 'contingencia'),'false')::bool;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'Consulto la de contingencia', i_id_venta);
	
		if  r_venta.id is null then
			v_codigo_respuesta 	:= 'ERROR';
			v_texto_log 		:= 'No existe la venta';
			v_id_movimiento		:= -1;
		elsif r_venta.sincronizado = 1 then
			v_codigo_respuesta 	:= 'ERROR';
			v_texto_log 		:= 'La venta ya se encuentra sincronizada';	
			v_id_movimiento		:= -1;	
		elsif r_venta_detalle.id is null then
			v_codigo_respuesta 	:= 'ERROR';
			v_texto_log 		:= 'La venta no tiene detalle';		
			v_id_movimiento		:= -1;
		elsif r_venta_medio_pagos.valor_total_medios_pago > r_venta.valor_total_venta then
			v_codigo_respuesta 	:= 'ERROR';
			v_texto_log 		:= 'El valor de los medios de pagos supera el total de la venta';
			v_id_movimiento		:= -1;
		else
			v_texto_log := '--------------- CONSECUTIVO -------------';
			insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
			raise notice '%  %', v_nombre_up, v_texto_log;
		
			select *
			  into v_codigo_respuesta_consecutivo, v_texto_respuesta_consecutivo, v_id_consecutivo, v_consecutivo_actual, v_prefijo, v_consecutivo_completo, v_json_consecutivo, v_consecutivo_lazoexpressregistry
			  from fnc_generar_consecutivo_movimiento(i_id_venta		=> i_id_venta
			 										, i_ind_contigencia	=> v_ind_contngencia
			 										, i_id_equipo 		=> r_equipos.id
			 										, i_id_producto		=> r_venta_detalle.productos_id
			 										);
			insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'Se genera el consecutivo', i_id_venta);
			
			 -- Se valida si viene el consecutivo 
			if v_codigo_respuesta_consecutivo = 'ERROR' then 
				v_codigo_respuesta 	:= v_codigo_respuesta_consecutivo;
				v_texto_log 		:= v_texto_respuesta_consecutivo;
				v_id_movimiento		:= -1;
				insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'ERROR Se genera el consecutivo', i_id_venta);
				
			elsif v_consecutivo_actual is not null then 
				raise notice '% v_id_consecutivo: %, v_consecutivo_actual: %, v_prefijo: %, v_consecutivo_completo: % v_consecutivo_lazoexpressregistry: % ',v_nombre_up,  v_id_consecutivo, v_consecutivo_actual, v_prefijo, v_consecutivo_completo, v_consecutivo_lazoexpressregistry;
			 	insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'Se valida si ya la venta esta registrada en movimientos', i_id_venta); 
				
				-- Se valida si ya la venta esta registrada en movimientos
				select id 
				 into v_count_venta_movimiento
				 from ct_movimientos 	cm 
				where empresas_id 		= r_equipos.empresas_id
				  and tipo				= v_tipo_movimiento
				  and consecutivo		= v_consecutivo_actual
				  and remoto_id			= i_id_venta
				  and prefijo			= v_prefijo
				  and equipos_id		= r_equipos.id;
				insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_count_venta_movimiento: ' || coalesce(v_count_venta_movimiento,0), i_id_venta);
				raise notice '% v_count_venta_movimiento: %', v_nombre_up, coalesce(v_count_venta_movimiento,0);
				
				if v_count_venta_movimiento > 0 then 
					v_codigo_respuesta 	:= 'ERROR';
					v_texto_log 		:= 'La venta ya se encuentra registrada en movimientos';
					v_id_movimiento		:= -1;
					insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'La venta ya se encuentra registrada en movimientos', i_id_venta);
					raise notice '% texto_log: %', v_nombre_up, texto_log;
				else
					v_json_atributos	:= fnc_generar_json_atributo_movimiento (i_id_venta => i_id_venta, i_json_info_consecutivo => v_json_consecutivo);					 
					v_ano				:= extract(year from r_venta.fecha_fin);
					v_mes				:= extract(month from r_venta.fecha_fin);
					v_dia				:= extract(day from r_venta.fecha_fin);
					v_impreso			:= 'N';
					
					v_texto_log 		:= 'v_json_atributos' || coalesce(v_json_atributos::text, '');
					insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
					raise notice '% v_json_atributos: %', v_nombre_up, v_texto_log;
					
					select sd.id, 			sd.bodegas_id 
				   	  into v_origen_id,		v_id_bodega
				      from ventas 							v 
				      join surtidores 						s on v.surtidor = s.surtidor
				      join surtidores_detalles 				sd on s.id = sd.surtidores_id 
				     where v.id								= i_id_venta
				       and sd.cara 							= v.cara 
				       and sd.manguera						= v.manguera 
				       and sd.surtidor 						= v.surtidor;
				    
				    v_texto_log := 'v_origen_id ' || coalesce(v_origen_id::text, '') || ' v_id_bodega: ' || v_id_bodega;
					insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
					raise notice '%  %', v_nombre_up, v_texto_log;
				
					v_texto_log := 'r_venta.valor_total_venta: ' || coalesce(r_venta.valor_total_venta,-1) || ' r_venta.id_tipo_venta: '  || coalesce(r_venta.id_tipo_venta, 999);
					insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
					raise notice '%  %', v_nombre_up, v_texto_log;
				
				 	-- Si la venta es tipo consumo propio el total de la venta es cero (0)
				 	if r_venta.id_tipo_venta = 3 then 
				 		r_venta.valor_total_venta := 0;
				 	end if;
				 	
					v_texto_log := 'Consulta de persona - persona identificación de atributos: ' || coalesce(v_json_atributos::json ->> 'personas_identificacion'::text, '');
					insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
					raise notice '%  %', v_nombre_up, v_texto_log;		
					
					/*if (v_json_atributos::json ->> 'personas_identificacion')::text is not null then
						select id 
						  into v_id_persona
						  from ct_personas p
						 where identificacion = (v_json_atributos::json ->> 'personas_identificacion')::text;							
					end if;*/
				
					v_texto_log := '--------------- MOVIMIENTO -------------';
					insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
					raise notice '%  %', v_nombre_up, v_texto_log;
				
					begin
						insert into ct_movimientos (empresas_id,				tipo,						estado_movimiento,				estado
												 , fecha,						consecutivo,				responsables_id,				personas_id
												 , terceros_id,					costo_total,				venta_total,					impuesto_total
												 , descuento_total,				sincronizado,				equipos_id,						remoto_id
												 , atributos,					impreso,					movimientos_id,					uso_dolar
												 , ano,							mes,						dia,							jornadas_id
												 , origen_id,					prefijo,					id_tipo_venta)
										   values (r_equipos.empresas_id,		v_tipo_movimiento,			v_estado_movimiento,			'A'
												 , r_venta.fecha_fin,			v_consecutivo_actual,		r_venta.operario_id,		 	v_id_persona
												 , null,						0,							r_venta.valor_total_venta,		0
												 , 0,							2,							r_equipos.id,					i_id_venta
												 , v_json_atributos,			v_impreso,					null,							0
												 , v_ano,						v_mes,						v_dia,							r_venta.jornada_id
												 , v_origen_id,					v_prefijo,					r_venta.id_tipo_venta
											) returning id into v_id_movimiento;
					
						v_texto_log := 'Se inserta el movimiento: ' || coalesce(v_id_movimiento::text, '');
						insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
						raise notice '% %', v_nombre_up, v_texto_log;
					exception
						when others then
							get STACKED diagnostics  
								v_state   = RETURNED_SQLSTATE,
								v_msg     = MESSAGE_TEXT,
								v_detail  = PG_EXCEPTION_DETAIL,
								v_hint    = PG_EXCEPTION_HINT;
								v_error	:= 'Insert ct_movimientos ' || v_state || ' ' || v_msg || ' ' || v_detail || ' ' || v_hint;
								v_codigo_respuesta 	:= 'ERROR';	
								v_texto_log			:= v_error;
								v_json_respuesta := ' {"codigo_respuesta":"' 		|| coalesce(v_codigo_respuesta, '') 
													|| '", "texto_respuesta":"' 		|| coalesce(v_texto_log, '') 
													|| '", "id_movimiento":null'
													|| ', "tipo_movimiento":null' 
													|| ', "consecutivo":null' 
													|| ', "prefijo_consecutivo":null'
													|| ', "atributos_movimientos":null'
													|| ', "impreso":null'				||'}';
								o_json_respuesta := v_json_respuesta;
								insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_codigo_respuesta || ' ' || v_texto_log, i_id_venta);
								raise notice '% %', v_nombre_up, v_json_respuesta;						
								
					end; -- Fin insert ct_movimientos
					
					
					v_texto_log := '--------------- MOVIMIENTO DETALLE -------------';
					insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
					raise notice '%  %', v_nombre_up, v_texto_log;
					if v_id_movimiento is not null and v_id_movimiento > 0 then 
						-- Se inserta el detalle 
						for c_ventas_detalle in (select vd.*, s.factor_precio, s.factor_volumen_parcial, s.factor_importe_parcial
													  , importe (s.factor_precio , vd.precio) 					precio_importe
													  , importe (s.factor_volumen_parcial, vd.cantidad_precisa) cantidad_precisa_importe
													  , importe (s.factor_importe_parcial, vd.total) 			total_importe
												   from ventas_detalles vd
												   join ventas 			v on v.id 		= vd.ventas_id 
												   join surtidores 		s on v.surtidor = s.surtidor
												  where ventas_id = i_id_venta) loop 
							
							v_texto_log := 'c_ventas_detalle.precio: ' || c_ventas_detalle.precio || ' factor_precio: ' || c_ventas_detalle.factor_precio || ' precio_importe: ' || c_ventas_detalle.precio_importe;
							insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
							raise notice '% %', v_nombre_up, v_texto_log;
						
							v_texto_log := 'c_ventas_detalle.cantidad_precisa: ' || c_ventas_detalle.cantidad_precisa || ' factor_volumen_parcial: ' || c_ventas_detalle.factor_volumen_parcial || ' cantidad_precisa_importe: ' || c_ventas_detalle.cantidad_precisa_importe;
							insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
							raise notice '% %', v_nombre_up, v_texto_log;
						
							v_texto_log := 'c_ventas_detalle.total: ' || c_ventas_detalle.total || ' factor_importe_parcial: ' || c_ventas_detalle.factor_importe_parcial || ' total_importe: ' || c_ventas_detalle.total_importe;
							insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
							raise notice '% %', v_nombre_up, v_texto_log;
					
							v_json_atributos_detalle := '{}';
							if v_count_insert_movimiento_detalle > 1 then
								v_json_data_detalle :=  v_json_data_detalle || ',' ;
							end if;
							
							-- Se consulta la información del producto
							select p.descripcion nombre_producto, p.unidad_medida_id, p.plu
							  into r_productos
							  from productos p 
							 where id = c_ventas_detalle.productos_id;
							
							v_texto_log := 'r_venta.valor_total_venta: ' || r_venta.valor_total_venta || ' r_venta.id_tipo_venta: '  || r_venta.id_tipo_venta;
							insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
							raise notice '%  %', v_nombre_up, v_texto_log;
						
							-- Si la venta es tipo consumo propio el total de la venta es cero (0)
						 	if r_venta.id_tipo_venta = 3 then 
						 		c_ventas_detalle.total_importe := 0;
						 	end if;
						 	
						 	v_texto_log := 'r_venta.datos_precio_diferencial: ' || coalesce(r_venta.datos_precio_diferencial::text, '');
							insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
							raise notice '%  %', v_nombre_up, v_texto_log;
						
						 	-- Si la venta es tipo cliente propio (10) se valida si trae precio diferencial		
						 	if r_venta.id_tipo_venta = 10 and r_venta.datos_precio_diferencial is not null then								
	                            for c_precios_diferenciales in (select x."pos_id"::int id_familia_pos, x."valor"::int valor_diferencial
	                            								  from json_to_recordset((r_venta.datos_precio_diferencial::json->>'precios')::json) as x("familiaId" int, "valor" text, "pos_id" text)
	                                                            )loop
									v_texto_log := 'c_precios_diferenciales.id_familia_pos: ' || coalesce(c_precios_diferenciales.id_familia_pos::text, '') || ' c_precios_diferenciales.valor_diferencial: ' || coalesce(c_precios_diferenciales.valor_diferencial::text, '');
									insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
									raise notice '%  %', v_nombre_up, v_texto_log; 
								
									if c_precios_diferenciales.id_familia_pos::int = (v_json_atributos ->> 'familiaId')::int  then
		                            	c_ventas_detalle.total_importe := c_ventas_detalle.total_importe + (c_precios_diferenciales.valor_diferencial * c_ventas_detalle.cantidad_precisa_importe);
		                            	v_total_valor_venta_precio_diferencial := v_total_valor_venta_precio_diferencial + c_ventas_detalle.total_importe;		                            
		                            end if;
	                            end loop;
						 	else
			                	v_total_valor_venta_precio_diferencial := c_ventas_detalle.total_importe;	
						 	end if;
                            
                            v_texto_log := 'c_ventas_detalle.total_importe: ' ||c_ventas_detalle.total_importe || ' v_total_valor_venta_precio_diferencial: ' || v_total_valor_venta_precio_diferencial;
							insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
							raise notice '%  %', v_nombre_up, v_texto_log;
						
							begin
								insert into ct_movimientos_detalles(movimientos_id,						bodegas_id,						cantidad,									costo_producto
																  , precio,								descuentos_id,					descuento_calculado,						fecha
																  , ano,								mes,							dia,										remoto_id
																  , sincronizado,						sub_total,						sub_movimientos_detalles_id,				unidades_id
																  , productos_id,						atributos)
															values (v_id_movimiento,					v_id_bodega,					c_ventas_detalle.cantidad_precisa_importe,	0
																  , c_ventas_detalle.precio_importe,	0,								0,											r_venta.fecha_fin
																  , v_ano,								v_mes,							v_dia,										c_ventas_detalle.id
																  , 0,									c_ventas_detalle.total_importe,	c_ventas_detalle.id,						r_productos.unidad_medida_id
																  , c_ventas_detalle.productos_id,		v_json_atributos_detalle
																) returning id into v_id_movimiento_detalle;
								
								v_count_insert_movimiento_detalle	:= v_count_insert_movimiento_detalle + 1;
								v_texto_log := 'Se inserta el movimiento detalle : ' || v_id_movimiento_detalle || '  v_count_insert_movimiento_detalle: ' || v_count_insert_movimiento_detalle;
								insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
								raise notice '% %', v_nombre_up, v_texto_log;
								
								-- Se crea el json para el json date
								v_json_data_detalle :=  v_json_data_detalle || '{"descripcion_producto":"'	|| coalesce(r_productos.nombre_producto, '')
																			|| '","precio":'				|| coalesce(c_ventas_detalle.precio_importe, -1)
																			|| ',"cantidad":'				|| coalesce(c_ventas_detalle.cantidad_precisa_importe, -1)
																			|| ',"unidad_productos":"'		|| coalesce(r_productos.unidad_medida_id, -1)
																			|| '","sub_total":'				|| coalesce(c_ventas_detalle.total_importe,  -1)
																			|| ',"plu":"'					|| coalesce(r_productos.plu, '')
																			|| '"}';
								
								v_texto_log := 'v_json_data_detalle: ' || coalesce(v_json_data_detalle::text, '') ;
								insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
								raise notice '%  %', v_nombre_up, v_texto_log;
								
							exception
								when others then
									get STACKED diagnostics  
										v_state   = RETURNED_SQLSTATE,
										v_msg     = MESSAGE_TEXT,
										v_detail  = PG_EXCEPTION_DETAIL,
										v_hint    = PG_EXCEPTION_HINT;
										v_error	:= 'Insert ct_movimientos_detalles ' || v_state || ' ' || v_msg || ' ' || v_detail || ' ' || v_hint;
										v_codigo_respuesta 	:= 'ERROR';
										v_texto_log			:= v_error;
										v_json_respuesta := ' {"codigo_respuesta":"' 		|| coalesce(v_codigo_respuesta, '') 
															|| '", "texto_respuesta":"' 		|| coalesce(v_texto_log, '') 
															|| '", "id_movimiento":null'
															|| ', "tipo_movimiento":null' 
															|| ', "consecutivo":null' 
															|| ', "prefijo_consecutivo":null'
															|| ', "atributos_movimientos":null'
															|| ', "impreso":null'				||'}';
										o_json_respuesta := v_json_respuesta;
										insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_codigo_respuesta || ' ' || v_texto_log, i_id_venta);
										raise notice '% %', v_nombre_up, v_json_respuesta;						
										
							end; -- Fin insert ct_movimientos_detalles
							
						end loop; -- Fin c_ventas_detalle
						
						v_texto_log := '--------------- MOVIMIENTO MEDIO DE PAGO -------------';
						insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
						raise notice '%  %', v_nombre_up, v_texto_log;
					
						v_texto_log := 'r_venta_medio_pagos.cantidad_medios_pago ' || coalesce(r_venta_medio_pagos.cantidad_medios_pago,0);
						insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
						raise notice '%  %', v_nombre_up, v_texto_log;
						
						
						v_texto_log := 'r_venta.valor_total_venta: ' || r_venta.valor_total_venta || ' r_venta.id_tipo_venta: '  || r_venta.id_tipo_venta;
						insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
						raise notice '%  %', v_nombre_up, v_texto_log;
					
						-- Si la venta es tipo consumo propio (3) el total de la venta es cero (0)
						if r_venta.id_tipo_venta = 3 then 
					 		v_sub_total := 0;
					 	-- Si la venta es tipo Cliente Propio (10) 
					 	elsif r_venta.id_tipo_venta = 10 then 
					 		v_sub_total := v_total_valor_venta_precio_diferencial;
					 		update ct_movimientos set venta_total = v_sub_total where id = v_id_movimiento;
					 	elsif r_venta.id_tipo_venta = 4 then
					 		v_id_medio_pago_x_defecto := r_venta.medios_pagos_id;
					 	elsif r_venta.id_tipo_venta = -1 and (v_id_medio_pago_atributos is not null or v_id_medio_pago_atributos > 0 ) then
					 		v_id_medio_pago_x_defecto := v_id_medio_pago_atributos;
					 		v_sub_total := r_venta.valor_total_venta;
					 	else 
					 		v_sub_total := r_venta.valor_total_venta;
					 	end if;
					 
					 	v_texto_log := 'v_sub_total: ' || v_sub_total;
						insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
						raise notice '%  %', v_nombre_up, v_texto_log;
					 	
				 		if r_venta.id_tipo_venta = 6 or r_venta.id_tipo_venta = 7 then
				 			raise notice '%  entro al loop', v_nombre_up;
				 			for c_ventas_medios_pago in (select a.medios_pagos_id
															  , a.valor_recibido
															  , a.valor_cambio
															  , a.valor_total
															  , coalesce (a.numero_comprobante , '') numero_comprobante
															  , b.descripcion 
														   from ventas_medios_pagos	a
														   join medios_pagos		b on a.medios_pagos_id = b.id 
														  where ventas_id = i_id_venta) loop
								
								if v_count_insert_movimiento_medios_pago > 1 then
									v_json_data_medio_pago :=  v_json_data_medio_pago || ',' ;
								end if;
						
								v_texto_log := 'c_ventas_medios_pago.medios_pagos_id: ' || c_ventas_medios_pago.medios_pagos_id || ' c_ventas_medios_pago.descripcion: ' || c_ventas_medios_pago.descripcion || ' c_ventas_medios_pago.valor_recibido: ' || c_ventas_medios_pago.valor_recibido;
								insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
								raise notice '%  %', v_nombre_up, v_texto_log;
							
								begin
									insert into ct_movimientos_medios_pagos (ct_medios_pagos_id,							ct_movimientos_id,								valor_recibido,							valor_cambio
																		   , valor_total,									numero_comprobante,								moneda_local,							trm)
																	 values (c_ventas_medios_pago.medios_pagos_id,			v_id_movimiento,								c_ventas_medios_pago.valor_recibido,	c_ventas_medios_pago.valor_cambio
																		   , c_ventas_medios_pago.valor_total,				c_ventas_medios_pago.numero_comprobante,		'S',									0);
									v_count_insert_movimiento_medios_pago := v_count_insert_movimiento_medios_pago + 1;
									v_texto_log := 'Se inserta el movimiento medios pagos  v_count_insert_movimiento_medios_pago: ' || v_count_insert_movimiento_medios_pago;
									insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
									raise notice '%  %', v_nombre_up, v_texto_log;
								
									-- Se crea el json para el json date
									v_json_data_medio_pago :=  v_json_data_medio_pago 	|| '{"medio":'				|| coalesce(c_ventas_medios_pago.medios_pagos_id, -1)
																						|| ',"descripcion":"'		|| coalesce(c_ventas_medios_pago.descripcion, '')
																						|| '","valor_recibido":'	|| coalesce(c_ventas_medios_pago.valor_recibido, -1)
																						|| ',"valor_cambio":'		|| coalesce(c_ventas_medios_pago.valor_cambio, -1)
																						|| ',"numero_comprobante":"'|| coalesce(c_ventas_medios_pago.numero_comprobante,  '')
																						|| '"}';
									v_texto_log := 'v_json_data_medio_pago: ' || coalesce(v_json_data_medio_pago::text, '');
									insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
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
												insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_codigo_respuesta || ' ' || v_error, i_id_venta);
												raise notice 'prc_registro_movimiento %', v_codigo_respuesta || ' ' || v_error;
								end; -- Fin  ct_movimientos_medios_pagos
							end loop; -- c_ventas_medios_pago
					 	else
				 			raise notice '%  NO entro al loop', v_nombre_up;
				 			select mp.descripcion into v_descripcion_medio_pago from medios_pagos mp where id = v_id_medio_pago_x_defecto;
				 			raise notice '%  v_descripcion_medio_pago %', v_nombre_up, v_descripcion_medio_pago;
				 
				 			if v_descripcion_medio_pago is not null then
								begin
									insert into ct_movimientos_medios_pagos (ct_medios_pagos_id
																		   , ct_movimientos_id,				valor_recibido,				valor_cambio
																		   , valor_total,					numero_comprobante,			moneda_local,		trm)
																	 values (coalesce(v_id_medio_pago_x_defecto, 1)
																		   , v_id_movimiento,				v_sub_total,				0
																		   , v_sub_total,					'',							'S',				0);
									v_count_insert_movimiento_medios_pago := v_count_insert_movimiento_medios_pago + 1;
								
									v_texto_log := 'Se inserta el movimiento medios pagos  v_count_insert_movimiento_medios_pago: ' || v_count_insert_movimiento_medios_pago;
									insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
									raise notice '% %', v_nombre_up, v_texto_log;
								
									
									v_json_data_medio_pago := '{"medio":'					|| coalesce(v_id_medio_pago_x_defecto, 1)
															|| ',"descripcion":"'			|| coalesce(v_descripcion_medio_pago, 'EFECTIVO')
															|| '","valor_recibido":'		|| coalesce(r_venta.valor_total_venta, -1)
															|| ',"valor_cambio":'			|| '0'
															|| ',"numero_comprobante":""}';
		
									v_texto_log := 'v_json_data_medio_pago: ' || coalesce(v_json_data_medio_pago::text, '') ;
									insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
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
												v_texto_log			:= v_error;
												v_json_respuesta := ' {"codigo_respuesta":"' 		|| coalesce(v_codigo_respuesta, '') 
																	|| '", "texto_respuesta":"' 		|| coalesce(v_texto_log, '') 
																	|| '", "id_movimiento":null'
																	|| ', "tipo_movimiento":null' 
																	|| ', "consecutivo":null' 
																	|| ', "prefijo_consecutivo":null'
																	|| ', "atributos_movimientos":null'
																	|| ', "impreso":null'				||'}';
												o_json_respuesta := v_json_respuesta;
												insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_codigo_respuesta || ' ' || v_texto_log, i_id_venta);
												raise notice '% %', v_nombre_up, v_json_respuesta;												
								end; -- Fin  insert medios de pagos
							else	
								v_codigo_respuesta 	:= '---ERROR';
								v_texto_log			:= 'No se encontro medio de pago valido ' || v_id_medio_pago_x_defecto;
								v_json_respuesta := ' {"codigo_respuesta":"' 		|| coalesce(v_codigo_respuesta, '') 
													|| '", "texto_respuesta":"' 		|| coalesce(v_texto_log, '') 
													|| '", "id_movimiento":null'
													|| ', "tipo_movimiento":null' 
													|| ', "consecutivo":null' 
													|| ', "prefijo_consecutivo":null'
													|| ', "atributos_movimientos":null'
													|| ', "impreso":null'				||'}';
								o_json_respuesta := v_json_respuesta;
								insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_codigo_respuesta || ' ' || v_texto_log, i_id_venta);
								raise notice '% %', v_nombre_up, v_json_respuesta;						
								
							end if; -- FIN v_descripcion_medio_pago is not null
						end if; -- FIN v_descripcion_medio_pago is not null
						
					end if; -- v_id_movimiento is not null then 
					
				end if; -- v_count_venta_movimiento = 0
			
			end if; -- Fin validación consecutivo
		
		end if; -- Fin validaciones de venta
		
		v_texto_log := 'v_id_movimiento: ' || coalesce(v_id_movimiento::text, '') || ' v_count_insert_movimiento_detalle: ' || v_count_insert_movimiento_detalle || ' v_count_insert_movimiento_medios_pago: ' || v_count_insert_movimiento_medios_pago;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
		raise notice '%  %', v_nombre_up, v_texto_log;
						
		if v_id_movimiento is not null and v_id_movimiento > 0 and v_count_insert_movimiento_detalle > 0 /*and v_count_insert_movimiento_medios_pago > 0*/ then
			-- Se actualiza el consecutivo 
			if v_consecutivo_lazoexpressregistry and v_id_consecutivo is not null then 
				select * 
			      into v_respuesta_update 
			      from dblink_exec('pg_remote', 'update consecutivos set consecutivo_actual = ' || v_consecutivo_actual + 1 || ' where id = ' || v_id_consecutivo,false);	
			elsif v_consecutivo_lazoexpressregistry = false and v_id_consecutivo is not null then 
				update ct_consecutivos 
				   set consecutivo_actual = v_consecutivo_actual + 1
				where id = v_id_consecutivo;
			end if; 
			v_texto_log := 'Se actualiza el consecutivo v_id_consecutivo: ' || coalesce(v_id_consecutivo::text, '') || ' v_consecutivo_actual: ' || coalesce((v_consecutivo_actual + 1)::text, '');
			insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
			raise notice '%  %', v_nombre_up, v_texto_log;
		
			-- Se actualiza la venta a sincronizado = 1
			update ventas 
			  set sincronizado = 1
			 where id = i_id_venta;
			
			v_texto_log := 'Se actualiza la venta a sincronizado = 1';
			insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
			raise notice '%  %', v_nombre_up, v_texto_log;		
		
			select valor into v_empresa_dococumento_alias from parametros pa where codigo = 'empresa_dococumento_alias';
			v_json_data_detalle := '[' || v_json_data_detalle || ']';
			raise notice  'v_json_data_detalle FINAL %', v_json_data_detalle;
			insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_data_detalle ' || coalesce(v_json_data_detalle::text, ''), i_id_venta);
		
			v_json_data_medio_pago := '[' || v_json_data_medio_pago || ']';
			raise notice  'v_json_data_medio_pago FINAL %', v_json_data_medio_pago;
			insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_data_medio_pago ' || coalesce(v_json_data_medio_pago::text, ''), i_id_venta);
			
			v_texto_log := '--------------- TRANSMISIÓN -------------';
			insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
			raise notice '%  %', v_nombre_up, v_texto_log;
		
			v_ind_venta_fe 				:= fnc_validar_venta_electronica (i_id_venta => i_id_venta);
			
			v_texto_log := 'v_ind_venta_fe: ' || v_ind_venta_fe;
			insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
			raise notice '%  %', v_nombre_up, v_texto_log;
		
			if v_ind_venta_fe then 
				-- Registro en TRANSMISIÓN
				begin	
					select * 
					  into v_codigo_respuesta_registro_trans, v_texto_respuesta_registro_trans, v_json_response
				   	  from fnc_registrar_transmision(i_id_movimiento => v_id_movimiento);
				   	 
				   	v_texto_log := 'v_codigo_respuesta_registro_trans: ' || v_codigo_respuesta_registro_trans || ' v_texto_respuesta_registro_trans: ' || v_texto_respuesta_registro_trans || ' v_json_response: ' || v_json_response;
					insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
					raise notice '%  %', v_nombre_up, v_texto_log;
				exception
					when others then
						get STACKED diagnostics  
							v_state   = RETURNED_SQLSTATE,
							v_msg     = MESSAGE_TEXT,
							v_detail  = PG_EXCEPTION_DETAIL,
							v_hint    = PG_EXCEPTION_HINT;
							v_error	:= 'Registro en TRANSMISIÓN ' || v_state || ' ' || v_msg || ' ' || v_detail || ' ' || v_hint;
							v_codigo_respuesta 	:= 'ERROR';
							v_texto_log			:= v_error;
							v_json_respuesta := ' {"codigo_respuesta":"' 		|| coalesce(v_codigo_respuesta, '') 
												|| '", "texto_respuesta":"' 		|| coalesce(v_texto_log, '') 
												|| '", "id_movimiento":null'
												|| ', "tipo_movimiento":null' 
												|| ', "consecutivo":null' 
												|| ', "prefijo_consecutivo":null'
												|| ', "atributos_movimientos":null'
												|| ', "impreso":null'				||'}';
							o_json_respuesta := v_json_respuesta;
							insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_codigo_respuesta || ' ' || v_texto_log, i_id_venta);
							raise notice '% %', v_nombre_up, v_json_respuesta;						
							
				end; -- Fin registro en TRANSMISIÓN
			end if;
			
			v_texto_log := '--------------- JSON DATA -------------';
			insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
			raise notice '%  %', v_nombre_up, v_texto_log;
			begin
				v_json_data :=  ' {"razon_social":"' 				|| r_empresa.razon_social													|| '"'
							|| ',"alias":"'							|| r_empresa.alias															|| '"'
							|| ', "nit":"' 							|| r_empresa.nit 															|| '"'
							|| ', "direccion":"'					|| r_empresa.direccion														|| '"'
							|| ', "ciudades_descripcion":"' 		|| r_empresa.ciudades_descripcion											|| '"'
							|| ', "header":"' 						|| r_empresa.header 														|| '"'
							|| ', "footer":"' 						|| r_empresa.footer															|| '"'
							|| ', "jornada":"' 						|| r_venta.jornada_id 														|| '"'
							|| ', "islas":"' 						|| (v_json_atributos ->> 'islas')::text										|| '"'
							|| ', "surtidor":"' 					|| r_venta.surtidor 														|| '"'
							|| ', "cara":"' 						|| r_venta.cara 															|| '"'
							|| ', "manguera":"' 					|| r_venta.manguera															|| '"'
							|| ', "equipos_id":"' 					|| r_equipos.id																|| '"'
							|| ', "responsable_iva":"' 				|| fnc_consultar_parametro (i_codigo => 'RESPONSABLE_IVA')::text			|| '"'
							|| ', "cliente":' 						|| (v_json_atributos -> 'cliente')::text									|| ''
							|| ', "empresa_dococumento_alias":"' 	|| v_empresa_dococumento_alias 												|| '"'
							|| ', "consecutivo":"' 					|| v_consecutivo_actual 													|| '"'
							|| ', "fecha":"' 						|| r_venta.fecha_fin														|| '"'
							|| ', "impreso":"' 						|| fnc_consultar_parametro (i_codigo => 'IMPRIMIR_VENTA_FINALIZADA')		|| '"'
							|| ', "identificadorMovimiento":"' 		|| v_id_movimiento															|| '"'
							|| ', "pos_principal":"'				|| v_ind_pos_principal														|| '"'
							|| ', "operario":"' 					|| trim(coalesce((v_json_atributos ->> 'responsables_nombre')::text, ''))	|| '"'
							|| ', "vehiculo_placa":"' 				|| coalesce((v_json_atributos ->> 'vehiculo_placa')::text, '')				|| '"'
							|| ', "venta_total":"' 					|| r_venta.valor_total_venta 												|| '"'
							|| ', "atributos":' 					|| coalesce(v_json_atributos::text, 'null') 								|| ''
							|| ', "movimientos_detalles":' 			|| coalesce(v_json_data_detalle::text, 'null')								|| ''
							|| ', "medios_pagos":' 					|| coalesce(v_json_data_medio_pago::text, 'null')							|| ''
							|| ', "datos_fidelizacion":' 			|| coalesce(r_venta.datos_fidelizacion::text, 'null')						|| '' 
							--|| ', "datosFE":' 					|| coalesce(v_json_response::text, 'null')									|| ''
							|| ', "ind_modulo_fe_activo":"' 		|| coalesce(v_ind_modulo_fe_activo::text, 'null')							|| '"'
							|| ', "ind_fe_por_defecto":"' 			|| coalesce(v_ind_fe_por_defecto::text, 'null')								|| '"'
							|| ', "ind_fe_oblgatorio":"' 			|| coalesce(v_ind_fe_oblgatorio::text, 'null')								|| '"'
							|| ', "monto_minimo_fe":"' 				|| coalesce(v_monto_minimo_fe::text, 'null')								|| '"'
							|| ', "datosFE":' 						|| coalesce((v_json_atributos -> 'cliente')::text, 'null')					|| ''
							||'}';	

				insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_data: ' || coalesce(v_json_data::text, ''), i_id_venta);
				raise notice '% v_json_data %', v_nombre_up, v_json_data;
			exception
					when others then
						get STACKED diagnostics  
							v_state   = RETURNED_SQLSTATE,
							v_msg     = MESSAGE_TEXT,
							v_detail  = PG_EXCEPTION_DETAIL,
							v_hint    = PG_EXCEPTION_HINT;
							v_error	:= 'Registro en V_JSON_DATA ' || v_state || ' ' || v_msg || ' ' || v_detail || ' ' || v_hint;
							v_codigo_respuesta 	:= 'ERROR';
							v_texto_log			:= v_error;
							v_json_respuesta := ' {"codigo_respuesta":"' 		|| coalesce(v_codigo_respuesta, '') 
												|| '", "texto_respuesta":"' 		|| coalesce(v_texto_log, '') 
												|| '", "id_movimiento":null'
												|| ', "tipo_movimiento":null' 
												|| ', "consecutivo":null' 
												|| ', "prefijo_consecutivo":null'
												|| ', "atributos_movimientos":null'
												|| ', "impreso":null'				||'}';
							o_json_respuesta := v_json_respuesta;
							insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_codigo_respuesta || ' ' || v_texto_log, i_id_venta);
							raise notice '% %', v_nombre_up, v_json_respuesta;						
							
				end; -- Fin v_json_data
				
			update ct_movimientos set json_data = v_json_data where id = v_id_movimiento;
		
			v_codigo_respuesta 	:= 'OK';
			v_texto_log 		:= 'Se creo el movimiento : ' || v_id_movimiento;
			
			v_json_respuesta := ' {"codigo_respuesta":"' 		|| coalesce(v_codigo_respuesta, '') 
							|| '", "texto_respuesta":"' 		|| coalesce(v_texto_log, '') 
							|| '", "id_movimiento":'			|| coalesce(v_id_movimiento , '-1')  
							|| ', "tipo_movimiento":"' 			|| coalesce(v_tipo_movimiento, '-1')   
							|| '", "consecutivo":"' 			|| coalesce(v_consecutivo_actual, -1)
							|| '", "id_bodega":"' 			    || coalesce(v_id_bodega, -1)
							|| '", "prefijo_consecutivo":"' 	|| coalesce(v_prefijo, '')  
							|| '", "atributos_movimientos":'	|| coalesce(v_json_atributos, '{}')  
							|| ',  "impreso":"' 				|| coalesce(fnc_consultar_parametro (i_codigo => 'IMPRIMIR_VENTA_FINALIZADA') , 'S')  
							||'"}';		
			
			insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_json_respuesta: ' || coalesce(v_json_respuesta::text,''), i_id_venta);
			raise notice 'prc_registro_movimiento v_json_respuesta %', v_json_respuesta;
			
			v_texto_log := '--------------- PG_NOTIFIY -------------';
			insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
			raise notice '%  %', v_nombre_up, v_texto_log;
			begin
				select * into v_respuesta_notify from pg_notify ('movement_info', v_json_respuesta::text);
				insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, 'v_respuesta_notify: ' || coalesce(v_respuesta_notify::text,''), i_id_venta);
				raise notice 'v_respuesta_notify % ', v_respuesta_notify;
			exception
				when others then
					get STACKED diagnostics  
						v_state   = RETURNED_SQLSTATE,
						v_msg     = MESSAGE_TEXT,
						v_detail  = PG_EXCEPTION_DETAIL,
						v_hint    = PG_EXCEPTION_HINT;
						v_error	:= 'Registro en PG_NOTIFIY ' || v_state || ' ' || v_msg || ' ' || v_detail || ' ' || v_hint;
						v_codigo_respuesta 	:= 'ERROR';
						v_texto_log			:= v_error;
						v_json_respuesta := ' {"codigo_respuesta":"' 		|| coalesce(v_codigo_respuesta, '') 
											|| '", "texto_respuesta":"' 		|| coalesce(v_texto_log, '') 
											|| '", "id_movimiento":null'
											|| ', "tipo_movimiento":null' 
											|| ', "consecutivo":null' 
											|| ', "prefijo_consecutivo":null'
											|| ', "atributos_movimientos":null'
											|| ', "impreso":null'				||'}';
						o_json_respuesta := v_json_respuesta;
						insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_codigo_respuesta || ' ' || v_texto_log, i_id_venta);
						raise notice '% %', v_nombre_up, v_json_respuesta;						
						
			end; -- Fin pg_notify
			
			o_json_respuesta := v_json_respuesta;
		else
			v_json_respuesta := ' {"codigo_respuesta":"' 		|| coalesce(v_codigo_respuesta, '') 
							|| '", "texto_respuesta":"' 		|| coalesce(v_texto_log, '') 
							|| '", "id_movimiento":null'
							|| ', "tipo_movimiento":null' 
							|| ', "consecutivo":null' 
							|| ', "prefijo_consecutivo":null'
							|| ', "atributos_movimientos":null'
							|| ', "impreso":null'				||'}';
			o_json_respuesta := v_json_respuesta;
			insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, '----------- v_json_respuesta: ' || coalesce(v_json_respuesta::text,''), i_id_venta);
			raise notice '% v_json_respuesta %', v_nombre_up, v_json_respuesta;
			
		end if; -- FIn v_id_movimiento is not null and v_id_movimiento > 0
		
		---------------------------------------------------------------------------------------------------------------------	
		v_fecha_hora_fin := clock_timestamp();
		v_duracion_proceso	:= age(v_fecha_hora_fin, v_fecha_hora_inicio);
			
		v_texto_log := 'Salio. i_id_venta: ' || i_id_venta || ' Hora Inicio: ' || v_fecha_hora_inicio  || ' Hora Fin: ' || v_fecha_hora_fin  || ' Duración: ' || v_duracion_proceso;
		insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_texto_log, i_id_venta);
		raise notice 'v_texto_log % ', v_texto_log ;
	
	exception
		when others then
			get STACKED diagnostics  
	        	v_state   = RETURNED_SQLSTATE,
				v_msg     = MESSAGE_TEXT,
				v_detail  = PG_EXCEPTION_DETAIL,
				v_hint    = PG_EXCEPTION_HINT;
				v_error				:= v_state || ' ' || v_msg || ' ' || v_detail || ' ' || v_hint;
	           	v_codigo_respuesta 	:= 'ERRORR';
				v_id_movimiento		:= -1;
				v_json_respuesta := ' {"codigo_respuesta":"'	|| v_codigo_respuesta	|| '"
									 , "texto_respuesta":"' 	|| v_error 				|| '"
									 , "id_movimiento":' 		|| v_id_movimiento 		|| '}';
				o_json_respuesta := v_json_respuesta;
		
				insert into logs (nombre_up, texto_log, id_proceso) values (v_nombre_up, v_codigo_respuesta || ' ' || v_error, i_id_venta);
				raise notice 'prc_registro_movimiento %', v_codigo_respuesta || ' ' || v_json_respuesta;
				
	end;
$procedure$
;