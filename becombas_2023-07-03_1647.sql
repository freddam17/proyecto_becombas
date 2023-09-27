--
-- PostgreSQL database dump
-- vamos a verificar si podemos hacer los cambios y hacer push al servidor web

-- Dumped from database version 12.3
-- Dumped by pg_dump version 12.3
--
SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: becombas; Type: SCHEMA; Schema: -; Owner: aguzman
--

CREATE SCHEMA becombas;


ALTER SCHEMA becombas OWNER TO aguzman;

--
-- Name: pdcompar; Type: TYPE; Schema: becombas; Owner: aguzman
--

CREATE TYPE becombas.pdcompar AS (
	va_datori character varying,
	va_datdes character varying
);


ALTER TYPE becombas.pdcompar OWNER TO aguzman;

--
-- Name: pdcompar2; Type: TYPE; Schema: becombas; Owner: aguzman
--

CREATE TYPE becombas.pdcompar2 AS (
	va_datori character varying,
	va_datdes character varying,
	va_sintax character varying
);


ALTER TYPE becombas.pdcompar2 OWNER TO aguzman;

--
-- Name: pdcompar3; Type: TYPE; Schema: becombas; Owner: aguzman
--

CREATE TYPE becombas.pdcompar3 AS (
	va_datcon character varying,
	va_datori character varying,
	va_datdes character varying,
	va_sintax character varying
);


ALTER TYPE becombas.pdcompar3 OWNER TO aguzman;

--
-- Name: f_actualizar_db(integer, integer); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_actualizar_db(p_id_basdat integer, p_id_compar integer) RETURNS SETOF character varying
    LANGUAGE plpgsql
    AS $$
begin
	if p_id_basdat = 1 then
		delete from becombas.tbcatori where id_compar = p_id_compar;
		delete from becombas.tbfunori where id_compar = p_id_compar;
		delete from becombas.tbtabori where id_compar = p_id_compar;
		delete from becombas.tbobjori where id_compar = p_id_compar;
	elseif p_id_basdat = 2 then
		delete from becombas.tbcatdes where id_compar = p_id_compar;
		delete from becombas.tbfundes where id_compar = p_id_compar;
		delete from becombas.tbtabdes where id_compar = p_id_compar;
		delete from becombas.tbobjdes where id_compar = p_id_compar;
   elseif p_id_basdat = 3 then
		delete from becombas.tbpaquet where id_compar = p_id_compar;
	end if;
	
	return;
end;
$$;


ALTER FUNCTION becombas.f_actualizar_db(p_id_basdat integer, p_id_compar integer) OWNER TO aguzman;

--
-- Name: f_comparar_db(); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_comparar_db() RETURNS character varying
    LANGUAGE plpgsql
    AS $$
declare
	v_rs_catalo record;
	v_rs_basrem record;
	v_rs_campos record;
	v_qr_prikey varchar;
	v_qr_cambio varchar := '';
	v_no_esquem varchar;
	v_no_tablas varchar;
	v_no_campos varchar;
	v_no_camcat varchar;
begin
	for v_no_esquem in
		select distinct(no_esquem)
		from fwrecurs.tbbasremest
		order by 1
	loop
		raise notice '%', v_no_esquem;
		if not exists(
			select nspname
			from pg_namespace
			where nspname = v_no_esquem
		) then
			-- codigo para crear esquema
			v_qr_cambio = v_qr_cambio || chr(13) ||
				'create schema ' || v_no_esquem || ';'
			;
		end if;
		
		for v_no_tablas in
			select distinct(no_tablas)
			from fwrecurs.tbbasremest
			where no_esquem = v_no_esquem
			order by 1
		loop
			v_no_camcat = null;
			raise notice '%', v_no_tablas;
			if exists(
				select tablename
				from pg_tables
				where schemaname = v_no_esquem
				and tablename = v_no_tablas
			) then
				for v_rs_catalo in
					select
						b.nspname as no_esquem,
						b.relname as no_tablas,
						a.attnum as nu_ordcol,
						a.attname as no_column,
						pg_catalog.format_type(a.atttypid, a.atttypmod) as ti_column,
						case when (
							select substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
							from pg_catalog.pg_attrdef d
							where d.adrelid = a.attrelid
							and d.adnum = a.attnum
							and a.atthasdef
						) is not NULL
						then
							'DEFAULT '|| (
								select substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
								from pg_catalog.pg_attrdef d
								where d.adrelid = a.attrelid
								and d.adnum = a.attnum
								and a.atthasdef
							)
						else
							''
						end as va_defaul,
						(case when a.attnum = 1 then tx.p_key else '' end) as p_key,
						case when a.attnotnull = true then 'N' else 'S' end as va_notnul
					from pg_catalog.pg_attribute a
						inner join (
							select
								c.oid,
								n.nspname,
								c.relname
							from pg_catalog.pg_class c
								left join pg_catalog.pg_namespace n on n.oid = c.relnamespace
							where c.relname = v_no_tablas
							and n.nspname = v_no_esquem
							order by 2, 3
						) b	on a.attrelid = b.oid
						inner join (
							select
								a.attrelid,
								max(a.attnum) as max_attnum
							from pg_catalog.pg_attribute a
							where a.attnum > 0
							and not a.attisdropped
							group by a.attrelid
						) e on a.attrelid = e.attrelid
						left join lateral (
							SELECT conname || ' ' || constrainddef as p_key
							FROM (
								SELECT conname, pg_get_constraintdef(c.oid) as constrainddef
								FROM pg_constraint c
								WHERE conrelid = (
									SELECT attrelid FROM pg_attribute
										WHERE attrelid = (
											SELECT oid FROM pg_class WHERE relname = b.relname
											AND relnamespace = (
												SELECT ns.oid
												FROM pg_namespace ns
												WHERE ns.nspname = b.nspname
											)
										) AND attname='tableoid'
									)
								)tx
							where constrainddef ilike '%PRIMARY KEY%'
						) tx on true
					where a.attnum > 0
					and not a.attisdropped
					and b.relname not ilike '%_pkey'
					and b.relname not ilike '%_key'
					and b.relname not ilike '%_seq'
					order by 1, 2, 3
				loop
					raise notice '%', v_rs_catalo.no_column;
					select * into v_rs_basrem
					from fwrecurs.tbbasremest
					where no_esquem = v_rs_catalo.no_esquem
					and no_tablas = v_rs_catalo.no_tablas
					and nu_ordcol = v_rs_catalo.nu_ordcol;
					
					-- CONTROLAMOS LOS CAMBIOS EN LOS CAMPOS EXISTENTES
					-- CAMPOS QUE NO CAMBIARON NOMBRE
					
					/*if v_rs_basrem.no_column is null then
						v_qr_cambio = v_qr_cambio || chr(13) ||
							'ALTER TABLE ' || v_no_esquem || '.' || v_no_tablas ||
							' ADD COLUMN ' || v_rs_basrem.no_column ||
							(case when v_rs_basrem.ti_column = 'char' then '"' || v_rs_basrem.ti_column || '"' || ';'	else v_rs_basrem.ti_column || ';' end) || ' ' ||
							(case when v_rs_basrem.va_notnul = 'N' then ' NOT NULL;' else ';' end);
					else*/
					if v_rs_catalo.no_column <> v_rs_basrem.no_column then
						v_qr_cambio = v_qr_cambio || chr(13) ||
							'ALTER TABLE ' || v_no_esquem || '.' || v_no_tablas ||
							' RENAME ' || v_rs_catalo.no_column || ' TO ' || v_rs_basrem.no_column || ';';
					end if;
					
					if v_rs_catalo.ti_column <> v_rs_basrem.ti_column then
						v_qr_cambio = v_qr_cambio || chr(13) ||
							'ALTER TABLE ' || v_no_esquem || '.' || v_no_tablas ||
							' ALTER COLUMN ' || v_rs_catalo.no_column || ' TYPE ' ||
							(case when v_rs_basrem.ti_column = 'char' then '"' || v_rs_basrem.ti_column || '"' || ';' else v_rs_basrem.ti_column || ';' end);
					end if;
					raise notice '%', v_qr_cambio;
					if v_rs_catalo.va_notnul <> v_rs_basrem.va_notnul then
						v_qr_cambio = v_qr_cambio || chr(13) ||
							'ALTER TABLE ' || v_no_esquem || '.' || v_no_tablas ||
							' ALTER COLUMN ' || v_rs_catalo.no_column ||
							(case when v_rs_basrem.va_notnul = 'N' then ' SET NOT NULL;' else ' DROP NOT NULL;' end);
					end if;

					if v_rs_catalo.va_defaul <> v_rs_basrem.va_defaul then
						v_qr_cambio = v_qr_cambio || chr(13) ||
							'ALTER TABLE ' || v_no_esquem || '.' || v_no_tablas ||
							' ALTER COLUMN ' || v_rs_catalo.no_column ||
							(case when v_rs_basrem.va_defaul <> '' then ' SET ' || v_rs_basrem.va_defaul || ';'  else ' DROP DEFAULT;' end);
					end if;
					
					if v_no_camcat is null then
						v_no_camcat = v_rs_catalo.no_column;
					else
						v_no_camcat = v_no_camcat || ',' || v_rs_catalo.no_column;
					end if;
					
				end loop;
				
				for v_rs_basrem in 
					select *
					from fwrecurs.tbbasremest
					where no_esquem = v_rs_catalo.no_esquem
					and no_tablas = v_rs_catalo.no_tablas
					and no_column not in(
						select no_column
						from regexp_split_to_table(v_no_camcat, ',') as no_column
					)
				loop
				raise notice '%', v_rs_basrem.no_column;
				raise notice '%', v_qr_cambio;
					v_qr_cambio = v_qr_cambio || chr(13) ||
						'ALTER TABLE ' || v_no_esquem || '.' || v_no_tablas ||
						' ADD COLUMN ' || v_rs_basrem.no_column || ' ' ||
						(case when v_rs_basrem.ti_column = 'char' then '"' || v_rs_basrem.ti_column || '"' else v_rs_basrem.ti_column end) ||
						(case when v_rs_basrem.va_notnul = 'N' then ' NOT NULL;' else ';' end)
					;
				end loop;
			else
				-- codigo para crear tabla
				v_qr_cambio = v_qr_cambio || chr(13) || chr(13) ||
					'CREATE TABLE ' || v_no_esquem || '.' || v_no_tablas || '(';
				
				v_no_campos := '';
				
				for v_rs_campos in
					select *
					from fwrecurs.tbbasremest
					where no_esquem = v_no_esquem
					and no_tablas = v_no_tablas
					order by nu_ordcol
				loop
					v_no_campos = v_no_campos || chr(13) ||
						v_rs_campos.no_column || ' ' || v_rs_campos.ti_column ||
						(case when v_rs_campos.va_notnul = 'N' then ' NOT NULL ' else '' end) ||
						coalesce( ' ' || nullif(v_rs_campos.va_defaul, ''), '') || ',';
				end loop;
				
				v_qr_cambio = v_qr_cambio || v_no_campos;
				
				v_qr_cambio = substring(v_qr_cambio, 1, length(v_qr_cambio) - 1) || chr(13) || ';'; 
				
				/*
				v_qr_prikey = (
					select 'CONSTRAINT ' || va_prikey
					from fwrecurs.tbbasremest
					where no_esquem = v_no_esquem
					and no_tablas = v_no_tablas
					and nu_ordcol = 1
				);
				
				v_qr_cambio = v_qr_cambio || chr(13) || v_qr_prikey || ';';
				*/
			end if;
			
		end loop;
		raise notice '%', v_qr_cambio;
	end loop;
	

	return v_qr_cambio;
end;
$$;


ALTER FUNCTION becombas.f_comparar_db() OWNER TO aguzman;

--
-- Name: f_comparar_db(integer); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_comparar_db(p_id_compar integer) RETURNS SETOF character varying
    LANGUAGE plpgsql
    AS $$
declare
	v_rs_tabori record;
	v_rs_tabdes record;
	v_rs_funori record;
	v_rs_fundes record;
	v_rs_catori record;
	v_rs_catdes record;
	v_qr_cambio varchar := '';
	v_no_esquem varchar;
begin
	for v_rs_tabori in
		select *
		from becombas.tbtabori
		where id_compar = p_id_compar
		order by no_esquem, no_tablas, nu_ordcol
	loop
		select * into v_rs_tabdes
		from becombas.tbtabdes
		where id_compar = p_id_compar
		and no_esquem = v_rs_tabori.no_esquem
		and no_tablas = v_rs_tabori.no_tablas
		and no_column = v_rs_tabori.no_column;
		
		if 	v_rs_tabdes.no_column is null then
			return next v_rs_tabori.no_esquem || '.' || v_rs_tabori.no_tablas || '.' || v_rs_tabori.no_column || ' (N)';
		elseif (v_rs_tabori.ti_column <> v_rs_tabdes.ti_column
				or v_rs_tabori.va_defaul <> v_rs_tabdes.va_defaul
				or v_rs_tabori.va_notnul <> v_rs_tabdes.va_notnul
		) then
			return next v_rs_tabori.no_esquem || '.' || v_rs_tabori.no_tablas || '.' || v_rs_tabori.no_column || ' (M)';
		end if;
	end loop;
	
	for v_rs_funori in
		select *
		from becombas.tbfunori
		where id_compar = p_id_compar
		order by no_esquem, no_funcio, ca_parfun, ls_parfun, ls_partyp
	loop
		select * into v_rs_fundes
		from becombas.tbfundes
		where id_compar = p_id_compar
		and no_esquem = v_rs_funori.no_esquem
		and no_funcio = v_rs_funori.no_funcio
		and ca_parfun = v_rs_funori.ca_parfun
		and ls_parfun = v_rs_funori.ls_parfun
		and ls_partyp = v_rs_funori.ls_partyp;
		
		if v_rs_fundes.no_funcio is null then
			return next v_rs_funori.no_esquem || '.' || v_rs_funori.no_funcio ||'('|| rtrim(ltrim(trim(v_rs_funori.ls_parfun), '{'), '}') ||') (N)';
		elseif (v_rs_funori.ls_parfun <> v_rs_fundes.ls_parfun or 
			v_rs_funori.ls_partyp <> v_rs_fundes.ls_partyp or 
			replace(replace(v_rs_funori.df_funcio, '	', ''), ' ', '') <> replace(replace(v_rs_fundes.df_funcio, '	', ''), ' ', '')
		) then
			return next v_rs_funori.no_esquem || '.' || v_rs_funori.no_funcio ||'('|| rtrim(ltrim(trim(v_rs_funori.ls_parfun), '{'), '}') ||')  (M)';
		end if;
	end loop;	
	
	for v_rs_catori in
		select *
		from becombas.tbcatori
		where id_compar = p_id_compar
		order by no_esquem, no_tablas
	loop
		select * into v_rs_catdes
		from becombas.tbcatdes
		where id_compar = p_id_compar
		and no_esquem = v_rs_catori.no_esquem
		and no_tablas= v_rs_catori.no_tablas;
	
		if v_rs_catori.js_conten <> v_rs_catdes.js_conten then
			return next v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas || '(M)';
		end if;
	end loop;
	
	return;
end;
$$;


ALTER FUNCTION becombas.f_comparar_db(p_id_compar integer) OWNER TO aguzman;

--
-- Name: f_comparar_db(character varying, character varying); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_comparar_db(p_no_esquem character varying, p_no_tablas character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
declare
	v_rs_tabdes record;
	v_rs_campos record;
	v_qr_prikey varchar;
	v_qr_cambio varchar := '';
	v_no_tabori varchar;
	v_no_tabdes varchar;
	p_no_esquem varchar;
	p_no_tablas varchar;
	v_no_campos varchar;
	v_no_camcat varchar;
begin
	select no_tablas into v_no_tabori
	from becombas.tbtabori
	where no_esquem = p_no_esquem
	and no_tablas = p_no_tablas
	limit 1;
	
	if v_no_tabori is null then
		raise exception 'Tabla no existe en el origen;';
	end if;
	
	select * into v_no_tabdes
	from becombas.tbtabdes
	where no_esquem = p_no_esquem
	and no_tablas = p_no_tablas;
	
	if v_no_tabdes is null then
		-- codigo para crear tabla
		v_qr_cambio = v_qr_cambio || chr(13) || chr(13) ||
			'CREATE TABLE ' || p_no_esquem || '.' || p_no_tablas || '(';

		v_no_campos := '';

		for v_rs_campos in
			select *
			from becombas.tbtabori
			where no_esquem = p_no_esquem
			and no_tablas = p_no_tablas
			order by nu_ordcol
		loop
			v_no_campos = v_no_campos || chr(13) ||
				v_rs_campos.no_column || ' ' || v_rs_campos.ti_column ||
				(case when v_rs_campos.va_notnul = 'N' then ' NOT NULL ' else '' end) ||
				coalesce( ' ' || nullif(v_rs_campos.va_defaul, ''), '') || ',';
		end loop;

		v_qr_cambio = v_qr_cambio || v_no_campos;

		v_qr_cambio = substring(v_qr_cambio, 1, length(v_qr_cambio) - 1) || chr(13) || ';';
	end if;
	
	for v_rs_campos in
		select *
		from becombas.tbtabori
		where no_esquem = p_no_esquem
		and no_tablas = p_no_tablas
		order by nu_ordcol
	loop
		select * into v_rs_tabdes
		from becombas.tbtabdes
		where no_esquem = v_rs_campos.no_esquem
		and no_tablas = v_rs_campos.no_tablas
		and no_column = v_rs_campos.no_column;
	
		if v_rs_tabdes.no_column is null then
			v_qr_cambio = v_qr_cambio || chr(13) ||
				'ALTER TABLE ' || p_no_esquem || '.' || p_no_tablas ||
				' ADD COLUMN ' || v_rs_campos.no_column || ' ' ||
				(case when v_rs_campos.ti_column = 'char' then '"' || v_rs_campos.ti_column || '"' else v_rs_campos.ti_column end) ||
				(case when v_rs_campos.va_notnul = 'N' then ' NOT NULL;' else ';' end)
			;
		elseif v_rs_campos.no_column <> v_rs_tabdes.no_column then
			v_qr_cambio = v_qr_cambio || chr(13) ||
				'ALTER TABLE ' || p_no_esquem || '.' || p_no_tablas ||
				' RENAME ' || v_rs_campos.no_column || ' TO ' || v_rs_campos.no_column || ';'
			;
		elseif v_rs_campos.ti_column <> v_rs_tabdes.ti_column then
			v_qr_cambio = v_qr_cambio || chr(13) ||
				'ALTER TABLE ' || p_no_esquem || '.' || p_no_tablas ||
				' ALTER COLUMN ' || v_rs_campos.no_column || ' TYPE ' ||
				(case when v_rs_campos.ti_column = 'char' then '"' || v_rs_campos.ti_column || '"' || ';' else v_rs_campos.ti_column || ';' end)
			;
		elseif v_rs_campos.va_notnul <> v_rs_tabdes.va_notnul then
			v_qr_cambio = v_qr_cambio || chr(13) ||
				'ALTER TABLE ' || p_no_esquem || '.' || p_no_tablas ||
				' ALTER COLUMN ' || v_rs_campos.no_column ||
				(case when v_rs_campos.va_notnul = 'N' then ' SET NOT NULL;' else ' DROP NOT NULL;' end)
			;
		elseif v_rs_campos.va_defaul <> v_rs_tabdes.va_defaul then
			v_qr_cambio = v_qr_cambio || chr(13) ||
				'ALTER TABLE ' || p_no_esquem || '.' || p_no_tablas ||
				' ALTER COLUMN ' || v_rs_campos.no_column ||
				(case when v_rs_campos.va_defaul <> '' then ' SET ' || v_rs_campos.va_defaul || ';'  else ' DROP DEFAULT;' end)
			;
		end if;
	end loop;
	
	return v_qr_cambio;
end;
$$;


ALTER FUNCTION becombas.f_comparar_db(p_no_esquem character varying, p_no_tablas character varying) OWNER TO aguzman;

--
-- Name: f_comparar_db(integer, integer, character varying); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_comparar_db(p_id_compar integer, p_ti_compar integer, p_va_compar character varying) RETURNS SETOF becombas.pdcompar
    LANGUAGE plpgsql
    AS $$
declare
	v_rs_tabori record;
	v_rs_tabdes record;
	v_rs_funori record;
	v_rs_fundes record;
	v_rs_catori record;
	v_rs_catdes record;
	v_rs_return becombas.pdcompar%rowtype;
	v_js_conori json;
	v_js_condes json;
	v_js_rowori varchar;
	v_js_rowdes varchar;
	v_nu_conori integer;
	v_nu_condes integer;
	v_ca_restos integer;
	v_ca_conori integer;
	v_ca_condes integer;
begin
	/*
	select becombas.f_comparar_db(
	35,	-- id_comparacion
	1,	-- tipo de comparacio (1: estructura, 2: funciones, 3:contenido)
	'esquema.tabla.columna' -- dato de comparacion
)
	*/
	if p_ti_compar = 1 then
		for v_rs_tabori in
			select *
			from becombas.tbtabori
			where id_compar = p_id_compar
			and no_esquem || '.' || no_tablas || '.' || no_column = p_va_compar
		loop
			select * into v_rs_tabdes
			from becombas.tbtabdes
			where id_compar = p_id_compar
			and no_esquem = v_rs_tabori.no_esquem
			and no_tablas = v_rs_tabori.no_tablas
			and no_column = v_rs_tabori.no_column;
			
			v_rs_return.va_datori = v_rs_tabori.no_column || ' ' ||
				v_rs_tabori.ti_column || ' ' || 
				(case when v_rs_tabori.va_notnul = 'N' then 'NOT NULL' else '' end) || ' ' ||
				v_rs_tabori.va_defaul;
			
			v_rs_return.va_datdes = v_rs_tabdes.no_column || ' ' ||
				v_rs_tabdes.ti_column || ' ' ||
				(case when v_rs_tabdes.va_notnul = 'N' then 'NOT NULL' else '' end) || ' ' ||
				v_rs_tabdes.va_defaul;
			
			return next v_rs_return;
		end loop;
	elseif p_ti_compar = 2 then
		for v_rs_funori in
			select *
			from becombas.tbfunori
			where id_compar = p_id_compar
			and no_esquem || '.' || no_funcio ||'('|| rtrim(ltrim(trim(ls_parfun), '{'), '}') ||')' = p_va_compar
		loop
			select * into v_rs_fundes
			from becombas.tbfundes
			where id_compar = p_id_compar
			and no_esquem = v_rs_funori.no_esquem
			and no_funcio = v_rs_funori.no_funcio
			and ca_parfun = v_rs_funori.ca_parfun
			and ls_parfun = v_rs_funori.ls_parfun
			and ls_partyp = v_rs_funori.ls_partyp;
			
			v_rs_return.va_datori = v_rs_funori.df_funcio;
			v_rs_return.va_datdes = v_rs_fundes.df_funcio;
			
			return next v_rs_return;
		end loop;
	elseif p_ti_compar = 3 then
		select * into v_rs_catori
		from becombas.tbcatori
		where id_compar = p_id_compar
		and no_esquem || '.' || no_tablas = p_va_compar;
		
		select * into v_rs_catdes
		from becombas.tbcatdes
		where id_compar = p_id_compar
		and no_esquem = v_rs_catori.no_esquem
		and no_tablas = v_rs_catori.no_tablas;
		
		v_js_conori = v_rs_catori.js_conten::json ->> 'ls_concat';
		v_js_condes = v_rs_catdes.js_conten::json ->> 'ls_concat';
		
		v_ca_conori = json_array_length(v_js_conori);
		v_ca_condes = json_array_length(v_js_condes);
				
		if v_js_conori is not null then
			for v_nu_conori in 1..json_array_length(v_js_conori)
			loop
				v_js_rowori = v_js_conori ->> (v_nu_conori - 1);
				v_js_rowdes = v_js_condes ->> (v_nu_conori - 1);
				
				continue when v_js_rowori = v_js_rowdes;
				
				v_rs_return.va_datori = v_js_rowori;
				v_rs_return.va_datdes = v_js_rowdes;

				return next v_rs_return;
			end loop;
		end if;
		
		if v_ca_condes > v_ca_conori then
			v_ca_restos = v_ca_condes - v_ca_conori;
			
			for v_nu_condes in 1..v_ca_restos
			loop
				v_js_rowdes = v_js_condes -> (v_ca_condes - v_nu_condes);
				
				v_rs_return.va_datori = null;
				v_rs_return.va_datdes = v_js_rowdes;
				
				return next v_rs_return;
			end loop;
		end if;
	end if;
	
	return;
end;
$$;


ALTER FUNCTION becombas.f_comparar_db(p_id_compar integer, p_ti_compar integer, p_va_compar character varying) OWNER TO aguzman;

--
-- Name: f_comparar_db(integer, character varying, character varying); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_comparar_db(p_id_compar integer, p_no_esquem character varying, p_no_tablas character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
declare
	v_rs_tabdes record;
	v_rs_campos record;
	v_qr_prikey varchar;
	v_qr_cambio varchar := '';
	v_no_tabori varchar;
	v_no_tabdes varchar;
	v_no_campos varchar;
	v_no_camcat varchar;
begin
	select no_tablas into v_no_tabori
	from becombas.tbtabori
	where id_compar = p_id_compar
	and no_esquem = p_no_esquem
	and no_tablas = p_no_tablas
	limit 1;
	
	if v_no_tabori is null then
		raise exception 'Tabla no existe en el origen;';
	end if;
	
	select * into v_no_tabdes
	from becombas.tbtabdes
	where id_compar = p_id_compar
	and no_esquem = p_no_esquem
	and no_tablas = p_no_tablas;
	
	if v_no_tabdes is null then
		-- codigo para crear tabla
		v_qr_cambio = v_qr_cambio || chr(13) || chr(13) ||
			'CREATE TABLE ' || p_no_esquem || '.' || p_no_tablas || '(';

		v_no_campos := '';

		for v_rs_campos in
			select *
			from becombas.tbtabori
			where id_compar = p_id_compar
			and no_esquem = p_no_esquem
			and no_tablas = p_no_tablas
			order by nu_ordcol
		loop
			v_no_campos = v_no_campos || chr(13) ||
				v_rs_campos.no_column || ' ' || v_rs_campos.ti_column ||
				(case when v_rs_campos.va_notnul = 'N' then ' NOT NULL ' else '' end) ||
				coalesce( ' ' || nullif(v_rs_campos.va_defaul, ''), '') || ',';
		end loop;

		v_qr_cambio = v_qr_cambio || v_no_campos;

		v_qr_cambio = substring(v_qr_cambio, 1, length(v_qr_cambio) - 1) || chr(13) || ';';
	end if;
	
	for v_rs_campos in
		select *
		from becombas.tbtabori
		where id_compar = p_id_compar
		and no_esquem = p_no_esquem
		and no_tablas = p_no_tablas
		order by nu_ordcol
	loop
		select * into v_rs_tabdes
		from becombas.tbtabdes
		where id_compar = p_id_compar
		and no_esquem = v_rs_campos.no_esquem
		and no_tablas = v_rs_campos.no_tablas
		and no_column = v_rs_campos.no_column;
	
		if v_rs_tabdes.no_column is null then
			v_qr_cambio = v_qr_cambio || chr(13) ||
				'ALTER TABLE ' || p_no_esquem || '.' || p_no_tablas ||
				' ADD COLUMN ' || v_rs_campos.no_column || ' ' ||
				(case when v_rs_campos.ti_column = 'char' then '"' || v_rs_campos.ti_column || '"' else v_rs_campos.ti_column end) ||
				(case when v_rs_campos.va_notnul = 'N' then ' NOT NULL;' else ';' end)
			;
		elseif v_rs_campos.no_column <> v_rs_tabdes.no_column then
			v_qr_cambio = v_qr_cambio || chr(13) ||
				'ALTER TABLE ' || p_no_esquem || '.' || p_no_tablas ||
				' RENAME ' || v_rs_campos.no_column || ' TO ' || v_rs_campos.no_column || ';'
			;
		elseif v_rs_campos.ti_column <> v_rs_tabdes.ti_column then
			v_qr_cambio = v_qr_cambio || chr(13) ||
				'ALTER TABLE ' || p_no_esquem || '.' || p_no_tablas ||
				' ALTER COLUMN ' || v_rs_campos.no_column || ' TYPE ' ||
				(case when v_rs_campos.ti_column = 'char' then '"' || v_rs_campos.ti_column || '"' || ';' else v_rs_campos.ti_column || ';' end)
			;
		elseif v_rs_campos.va_notnul <> v_rs_tabdes.va_notnul then
			v_qr_cambio = v_qr_cambio || chr(13) ||
				'ALTER TABLE ' || p_no_esquem || '.' || p_no_tablas ||
				' ALTER COLUMN ' || v_rs_campos.no_column ||
				(case when v_rs_campos.va_notnul = 'N' then ' SET NOT NULL;' else ' DROP NOT NULL;' end)
			;
		elseif v_rs_campos.va_defaul <> v_rs_tabdes.va_defaul then
			v_qr_cambio = v_qr_cambio || chr(13) ||
				'ALTER TABLE ' || p_no_esquem || '.' || p_no_tablas ||
				' ALTER COLUMN ' || v_rs_campos.no_column ||
				(case when v_rs_campos.va_defaul <> '' then ' SET ' || v_rs_campos.va_defaul || ';'  else ' DROP DEFAULT;' end)
			;
		end if;
	end loop;
	
	return v_qr_cambio;
end;
$$;


ALTER FUNCTION becombas.f_comparar_db(p_id_compar integer, p_no_esquem character varying, p_no_tablas character varying) OWNER TO aguzman;

--
-- Name: f_comparar_db_catalogos(integer); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_comparar_db_catalogos(p_id_compar integer) RETURNS SETOF character varying
    LANGUAGE plpgsql
    AS $$
declare
	v_rs_catori record;
	v_rs_catdes record;
begin
	for v_rs_catori in
		select *
		from becombas.tbcatori
		where id_compar = p_id_compar
		order by no_esquem, no_tablas
	loop
		select * into v_rs_catdes
		from becombas.tbcatdes
		where id_compar = p_id_compar
		and no_esquem = v_rs_catori.no_esquem
		and no_tablas = v_rs_catori.no_tablas
		and js_conten = v_rs_catori.js_conten
		;
	
		if (v_rs_catdes.no_tablas is null or
			replace(v_rs_catori.js_conten,' ', '') <> replace(v_rs_catdes.js_conten,' ', '')
		) then
			return next v_rs_catori.no_esquem || '.' ||v_rs_catori.no_tablas;
		end if;
	end loop;
	
	return;
end;
$$;


ALTER FUNCTION becombas.f_comparar_db_catalogos(p_id_compar integer) OWNER TO aguzman;

--
-- Name: f_comparar_db_funciones(integer); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_comparar_db_funciones(p_id_compar integer) RETURNS SETOF character varying
    LANGUAGE plpgsql
    AS $$
declare
	v_rs_funori record;
	v_rs_fundes record;
begin
	for v_rs_funori in
		select *
		from becombas.tbfunori
		where id_compar = p_id_compar
		--and no_esquem not in('fwlogneg', 'oqwebser', 'oqlogser')
		order by no_esquem, no_funcio, ca_parfun, ls_parfun, ls_partyp
	loop
		select * into v_rs_fundes
		from becombas.tbfundes
		where id_compar = p_id_compar
		and no_esquem = v_rs_funori.no_esquem
		and no_funcio = v_rs_funori.no_funcio
		and ca_parfun = v_rs_funori.ca_parfun
		and ls_parfun = v_rs_funori.ls_parfun
		and ls_partyp = v_rs_funori.ls_partyp;
	
		if (v_rs_fundes.no_funcio is null or
			v_rs_funori.ls_parfun <> v_rs_fundes.ls_parfun or 
			v_rs_funori.ls_partyp <> v_rs_fundes.ls_partyp or 
			replace(replace(v_rs_funori.df_funcio, '	', ''), ' ', '') <> replace(replace(v_rs_fundes.df_funcio, '	', ''), ' ', '')
		) then
			return next v_rs_funori.df_funcio || ';';
		end if;
	end loop;
	
	return;
end;
$$;


ALTER FUNCTION becombas.f_comparar_db_funciones(p_id_compar integer) OWNER TO aguzman;

--
-- Name: f_comparar_db_funciones(integer, character varying); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_comparar_db_funciones(p_id_compar integer, p_no_esquem character varying) RETURNS SETOF character varying
    LANGUAGE plpgsql
    AS $$
declare
	v_rs_funori record;
	v_rs_fundes record;
begin
	for v_rs_funori in
		select *
		from becombas.tbfunori
		where id_compar = p_id_compar
		--and (case when p_va_busque = '' then true else no_esquem ilike '%'|| p_va_busque ||'%' end)
		and no_esquem = p_no_esquem
		order by no_esquem, no_funcio, ca_parfun, ls_parfun, ls_partyp
	loop
		select * into v_rs_fundes
		from becombas.tbfundes
		where id_compar = p_id_compar
		and no_esquem = v_rs_funori.no_esquem
		and no_funcio = v_rs_funori.no_funcio
		and ca_parfun = v_rs_funori.ca_parfun
		and ls_parfun = v_rs_funori.ls_parfun
		and ls_partyp = v_rs_funori.ls_partyp;
	
		if (v_rs_fundes.no_funcio is null or
			v_rs_funori.ls_parfun <> v_rs_fundes.ls_parfun or 
			v_rs_funori.ls_partyp <> v_rs_fundes.ls_partyp or 
			replace(replace(v_rs_funori.df_funcio, '	', ''), ' ', '') <> replace(replace(v_rs_fundes.df_funcio, '	', ''), ' ', '')
		) then
			return next v_rs_funori.df_funcio || ';';
		end if;
	end loop;
	
	return;
end;
$$;


ALTER FUNCTION becombas.f_comparar_db_funciones(p_id_compar integer, p_no_esquem character varying) OWNER TO aguzman;

--
-- Name: f_comparar_db_sintaxis(integer); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_comparar_db_sintaxis(p_id_compar integer) RETURNS SETOF becombas.pdcompar2
    LANGUAGE plpgsql
    AS $$
declare
	v_rs_tabori record;
	v_rs_tabdes record;
	v_rs_funori record;
	v_rs_fundes record;
	v_rs_catori record;
	v_rs_catdes record;
	v_rs_prikey record;
	v_rs_coltip record;
	v_rs_campos record;
	v_rs_return becombas.pdcompar2%rowtype;
	v_js_conori json;
	v_js_condes json;
	v_js_rowori json;
	v_js_rowdes json;
	v_nu_conori integer;
	v_nu_condes integer;
	v_ca_restos integer;
	v_ca_conori integer;
	v_ca_condes integer;
	v_no_esquem varchar;
	v_no_tablas varchar;
	v_qr_return varchar;
	v_va_coltip varchar;
	v_va_prike1 varchar;
	v_va_prike2 varchar;
	v_va_prike3 varchar;
	v_va_prike4 varchar;
	v_qr_insert varchar;
	v_qr_actcam varchar;
	v_va_prikey varchar;
	v_il_insert_cam boolean;
	v_qr_insert_cam varchar;
	v_qr_insert_val varchar;
begin
	/*
		select * from becombas.f_comparar_db_sintaxis(45);
	*/
	-- REVISION DE ESTRUCTURAS
	for v_no_esquem in
		select distinct no_esquem
		from becombas.tbtabori
		where id_compar = p_id_compar
		order by no_esquem
	loop
		if exists (
			select distinct no_esquem from becombas.tbtabdes
			where id_compar = p_id_compar
			and no_esquem = v_no_esquem
		) then
			for v_no_tablas in
				select distinct no_tablas
				from becombas.tbtabori
				where id_compar = p_id_compar
				and no_esquem = v_no_esquem
				order by no_tablas
			loop
				if exists (
					select distinct no_tablas from becombas.tbtabdes
					where id_compar = p_id_compar
					and no_esquem = v_no_esquem
					and no_tablas = v_no_tablas
				) then
					for v_rs_tabori in
						select * from becombas.tbtabori
						where id_compar = p_id_compar
						and no_esquem = v_no_esquem
						and no_tablas = v_no_tablas
						order by nu_ordcol
					loop
						v_qr_return = null;
						
						select * into v_rs_tabdes
						from becombas.tbtabdes
						where id_compar = p_id_compar
						and no_esquem = v_no_esquem
						and no_tablas = v_no_tablas
						and no_column = v_rs_tabori.no_column;
						
						if v_rs_tabdes.no_column is null then -- ADD COLUMN
							v_qr_return = 'ALTER TABLE '|| v_no_esquem || '.' || v_no_tablas ||
								' ADD COLUMN '|| v_rs_tabori.no_column ||
								' ' || v_rs_tabori.ti_column ||
								' ' || v_rs_tabori.va_defaul ||
								(case when v_rs_tabori.va_notnul = 'N' then ' NOT NULL' else '' end) ||
								';'
							;
						else
							if v_rs_tabori.ti_column <> v_rs_tabdes.ti_column then -- MODIFY TYPE
								v_qr_return = 'ALTER TABLE '|| v_no_esquem || '.' || v_no_tablas ||
									' ALTER COLUMN '|| v_rs_tabori.no_column ||
									' TYPE ' || v_rs_tabori.ti_column ||
									(case when v_rs_tabori.va_notnul = 'N' then ' NOT NULL' else '' end) ||
									';'
								;
							end if;
							
							if v_rs_tabori.va_defaul <> v_rs_tabdes.va_defaul then -- MODIFY DEFAULT
								v_qr_return = coalesce(v_qr_return || ' ', '') ||
									'ALTER TABLE '|| v_no_esquem || '.' || v_no_tablas ||
									' ALTER COLUMN '|| v_rs_tabori.no_column ||
									' SET ' || v_rs_tabori.va_defaul ||
									(case when v_rs_tabori.va_notnul = 'N' then ' NOT NULL' else '' end) ||
									';';
							end if;
							
							if v_rs_tabori.va_notnul <> v_rs_tabdes.va_notnul then -- MODIFY NOT NULL
								v_qr_return = coalesce(v_qr_return || ' ', '') ||
									'ALTER TABLE '|| v_no_esquem || '.' || v_no_tablas ||
									' ALTER COLUMN '|| v_rs_tabori.no_column ||
									(case when v_rs_tabori.va_notnul = 'N' then ' SET NOT NULL' else ' DROP NOT NULL' end) ||
									';'
								;
							end if;
						end if;
						
						if v_qr_return is not null then
							v_rs_return.va_datori = v_rs_tabori.no_esquem ||'.'|| v_rs_tabori.no_tablas ||'.'|| v_rs_tabori.no_column;
							v_rs_return.va_datdes = v_rs_tabdes.no_esquem ||'.'|| v_rs_tabdes.no_tablas ||'.'|| v_rs_tabdes.no_column;
							v_rs_return.va_sintax = v_qr_return;

							return next v_rs_return;
						end if;
					end loop;	
				else	 -- CREATE TABLE
					
					if v_qr_return is not null then
						v_rs_return.va_datori = v_rs_tabori.no_esquem ||'.'|| v_rs_tabori.no_tablas;
						v_rs_return.va_datdes = v_rs_tabdes.no_esquem ||'.'|| v_rs_tabdes.no_tablas;
						v_rs_return.va_sintax = v_qr_return;

						return next v_rs_return;
					end if;
				end if;
			end loop;
		else	-- CREATE ESCHEMA
			
			if v_qr_return is not null then
				v_rs_return.va_datori = v_rs_tabori.no_esquem;
				v_rs_return.va_datdes = v_rs_tabdes.no_esquem;
				v_rs_return.va_sintax = v_qr_return;
				
				return next v_rs_return;
			end if;
		end if;
	end loop;
	
	-- REVISION DE FUNCIONES
	for v_rs_funori in
		select *
		from becombas.tbfunori
		where id_compar = p_id_compar
		order by no_esquem, no_funcio, ca_parfun
	loop
		v_qr_return = null;
		
		select * into v_rs_fundes
		from becombas.tbfundes
		where id_compar = p_id_compar
		and no_esquem = v_rs_funori.no_esquem
		and no_funcio = v_rs_funori.no_funcio
		and ca_parfun = v_rs_funori.ca_parfun
		and ls_parfun = v_rs_funori.ls_parfun
		and ls_partyp = v_rs_funori.ls_partyp;
		
		if v_rs_fundes.no_funcio is null then
			v_qr_return = v_rs_funori.df_funcio;
		elseif (v_rs_funori.ls_parfun <> v_rs_fundes.ls_parfun or 
			v_rs_funori.ls_partyp <> v_rs_fundes.ls_partyp or 
			replace(replace(v_rs_funori.df_funcio, '	', ''), ' ', '') <> replace(replace(v_rs_fundes.df_funcio, '	', ''), ' ', '')
		) then
			v_qr_return = v_rs_funori.df_funcio;
		end if;
		
		if v_qr_return is not null then
			v_rs_return.va_datori = v_rs_funori.no_esquem || '.' || v_rs_funori.no_funcio ||'('|| rtrim(ltrim(trim(v_rs_funori.ls_parfun), '{'), '}') ||')';
			v_rs_return.va_datdes = v_rs_funori.no_esquem || '.' || v_rs_fundes.no_funcio ||'('|| rtrim(ltrim(trim(v_rs_fundes.ls_parfun), '{'), '}') ||')';
			v_rs_return.va_sintax = v_qr_return || ';';
			
			return next v_rs_return;
		end if;
	end loop;
	
	-- REVISION DE CONTENIDO DE CATALOGOS
	for v_rs_catori in 
		select *
		from becombas.tbcatori
		where id_compar = p_id_compar
		order by no_esquem, no_tablas
	loop
		v_qr_return = '';
		v_qr_insert = '';
		v_qr_insert_cam = '';
		v_il_insert_cam = true;
		
		select * into v_rs_catdes
		from becombas.tbcatdes
		where id_compar = p_id_compar
		and no_esquem = v_rs_catori.no_esquem
		and no_tablas = v_rs_catori.no_tablas;
				
		v_js_conori = v_rs_catori.js_conten::json ->> 'ls_concat';
		v_js_condes = v_rs_catdes.js_conten::json ->> 'ls_concat';
		
		if v_rs_catdes.no_tablas is not null then
			if v_js_conori::varchar <> v_js_condes::varchar then
			-- TABLA CATALOGO ENCONTRADO Y CON DIFERENCIAS EN SU CONTENIDO
				-- IDENTIFICAMOS LA LLAVE PRIMARIA DE LA TABLA
				select
					tx[1] as co_prike1,
					tx[2] as co_prike2,
					tx[3] as co_prike3,
					tx[4] as co_prike4
				into v_rs_prikey
				from (
					select regexp_split_to_array(tx.no_column, ',')
					from (
						SELECT string_agg(no_column, ',') as no_column
						FROM becombas.tbtabori
						where id_compar = p_id_compar
						and no_esquem = v_rs_catori.no_esquem
						and no_tablas = v_rs_catori.no_tablas
						and va_prikey = 'S'
						group by id_compar, no_esquem, no_tablas
					) tx
				) as dt(tx);
				
				-- ARMAMOS LA SINTAXIS.
				if v_js_conori is not null then
					for v_js_rowori in
						select *
						from json_array_elements(v_js_conori::json)
					loop
						v_va_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1;
						v_va_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2;
						v_va_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3;
						v_va_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4;
						
						select tx.js_rowdes into v_js_rowdes
						from (
							select value as js_rowdes
							from json_array_elements(v_js_condes::json)
						) tx
						where (case when v_rs_prikey.co_prike1 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1 else true end)
						and (case when v_rs_prikey.co_prike2 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2 else true end)
						and (case when v_rs_prikey.co_prike3 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3 else true end)
						and (case when v_rs_prikey.co_prike4 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4 else true end)
						;
						
						if v_js_rowdes is not null then
						-- registro encontrado
							if v_js_rowori::varchar <> v_js_rowdes::varchar then
							-- registros diferentes
								-- generamos el update
								v_qr_actcam = '';
								
								for v_rs_coltip in
									select no_column, ti_column
									from becombas.tbtabori
									where id_compar = p_id_compar
									and no_esquem = v_rs_catori.no_esquem
									and no_tablas = v_rs_catori.no_tablas
									and va_prikey <> 'S'
									and no_column not ilike 'fe_%'
									and no_column <> 'co_archiv'
									order by nu_ordcol
								loop
									if (v_js_rowori ->> v_rs_coltip.no_column)::varchar <> (v_js_rowdes ->> v_rs_coltip.no_column)::varchar then
										v_qr_actcam = v_qr_actcam || 
											v_rs_coltip.no_column || ' = ''' || (v_js_rowori ->> v_rs_coltip.no_column)::varchar || '''::' || v_rs_coltip.ti_column || ',';
									end if;
								end loop;
								
								if v_qr_actcam <> '' then
									v_qr_actcam = substring(v_qr_actcam, 1, length(v_qr_actcam) - 1);
									
									v_qr_return = v_qr_return || 'Update ' || v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas || chr(10) ||
										' Set ' || coalesce(v_qr_actcam, 'v_qr_actcam salio null') || chr(10);
									
									for v_rs_campos in
										select no_column, ti_column, nu_ordcol
										from becombas.tbtabori
										where id_compar = p_id_compar
										and no_esquem = v_rs_catori.no_esquem
										and no_tablas = v_rs_catori.no_tablas
										and va_prikey = 'S'
										order by nu_ordcol
									loop
										v_va_prikey = v_js_rowori ->> v_rs_campos.no_column;
										
										if v_rs_campos.nu_ordcol = 1 then
											v_qr_return = v_qr_return || 'where ' || (case when v_rs_campos.no_column is not null then v_rs_campos.no_column || ' = ''' || v_va_prikey || '''::' || v_rs_campos.ti_column else ' false' end);
										else
											v_qr_return = v_qr_return || (case when v_rs_campos.no_column is not null then ' and ' || v_rs_campos.no_column || ' = ''' || v_va_prikey || '''::' || v_rs_campos.ti_column else '' end);
										end if;
									end loop;
									
									v_qr_return = v_qr_return || ';' || chr(10) || chr(10);
								end if;
							end if;
						else
						-- Registro no encontrado, por lo tanto generamos el insert
							v_qr_insert_val = '';
							
							for v_rs_coltip in
								select no_column, ti_column
								from becombas.tbtabori
								where id_compar = p_id_compar
								and no_esquem = v_rs_catori.no_esquem
								and no_tablas = v_rs_catori.no_tablas
								order by nu_ordcol
							loop
								if v_il_insert_cam then
									v_qr_insert_cam = v_qr_insert_cam || v_rs_coltip.no_column || ',';
								end if;
								
								v_qr_insert_val = v_qr_insert_val || '''' || coalesce((v_js_rowori ->> v_rs_coltip.no_column)::varchar, 'null') || '''::' || v_rs_coltip.ti_column || ',';
							end loop;
							
							if  v_qr_insert_val <> '' then
								v_qr_insert_val = substring(v_qr_insert_val, 1, length(v_qr_insert_val) - 1);

								if v_il_insert_cam then
									v_qr_insert_cam = substring(v_qr_insert_cam, 1, length(v_qr_insert_cam) - 1);
									v_qr_insert = v_qr_insert || 'Insert into ' || v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas ||' (' || v_qr_insert_cam || ')' || chr(10);
									v_qr_insert = v_qr_insert || '	  Select ' || v_qr_insert_val || chr(10);
									
									v_il_insert_cam = false;
								else
									v_qr_insert = v_qr_insert || 'Union Select ' || v_qr_insert_val || chr(10);
								end if;
							end if;
						end if;
					end loop;
					
					if v_qr_insert_val <> '' then
						v_qr_return = v_qr_return || v_qr_insert || ';';
					end if;
				end if;
			end if;
		else
		-- NUEVA TABLA CATALOGO
			for v_js_rowori in
				select *
				from json_array_elements(v_js_conori::json)
			loop
				v_qr_insert_val = null;
				
				for v_rs_coltip in
					select no_column, ti_column
					from becombas.tbtabori
					where id_compar = p_id_compar
					and no_esquem = v_rs_catori.no_esquem
					and no_tablas = v_rs_catori.no_tablas
					order by nu_ordcol
				loop
					if v_il_insert_cam then
						v_qr_insert_cam = v_qr_insert_cam || v_rs_coltip.no_column || ', ';
					end if;

					v_qr_insert_val = v_qr_insert_val || '''' || coalesce((v_js_rowori ->> v_rs_coltip.no_column)::varchar, 'null') || '''::' || v_rs_coltip.ti_column || ', ';
				end loop;
				
				if  v_qr_insert_val <> '' then
					v_qr_insert_val = substring(v_qr_insert_val, 1, length(v_qr_insert_val) - 1);
					
					if v_il_insert_cam then
						v_qr_insert_cam = substring(v_qr_insert_cam, 1, length(v_qr_insert_cam) - 1);
						
						v_qr_return = v_qr_return || 'Insert ' || v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas ||' (' || v_qr_insert_cam || ')' || chr(10);
						v_qr_return = v_qr_return || '	  Select ' || v_qr_insert_val || chr(10);
						
						v_il_insert_cam = false;
					else
						v_qr_return = v_qr_return || 'Union Select ' || v_qr_insert_val || chr(10);
					end if;
				end if;
			end loop;
			
			v_qr_return = v_qr_return || ';' ||chr(10);
		end if;
		
		if v_qr_return <> '' then
			v_rs_return.va_datori = v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas;
			v_rs_return.va_datdes = v_rs_catdes.no_esquem || '.' || v_rs_catdes.no_tablas;
			v_rs_return.va_sintax = v_qr_return;
			
			return next v_rs_return;
		end if;
	end loop;
	
	return;
end;
$$;


ALTER FUNCTION becombas.f_comparar_db_sintaxis(p_id_compar integer) OWNER TO aguzman;

--
-- Name: f_comparar_db_sintaxis(integer, integer); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_comparar_db_sintaxis(p_id_compar integer, p_ti_compar integer) RETURNS SETOF becombas.pdcompar2
    LANGUAGE plpgsql
    AS $$
declare
	v_rs_tabori record;
	v_rs_tabdes record;
	v_rs_funori record;
	v_rs_fundes record;
	v_rs_catori record;
	v_rs_catdes record;
	v_rs_prikey record;
	v_rs_coltip record;
	v_rs_campos record;
	v_rs_return becombas.pdcompar2%rowtype;
	v_js_conori json;
	v_js_condes json;
	v_js_rowori json;
	v_js_rowdes json;
	v_nu_conori integer;
	v_nu_condes integer;
	v_ca_restos integer;
	v_ca_conori integer;
	v_ca_condes integer;
	v_no_esquem varchar;
	v_no_tablas varchar;
	v_qr_return varchar;
	v_va_coltip varchar;
	v_va_prike1 varchar;
	v_va_prike2 varchar;
	v_va_prike3 varchar;
	v_va_prike4 varchar;
	v_qr_insert varchar;
	v_qr_actcam varchar;
	v_va_prikey varchar;
	v_co_bloque varchar;
	v_co_bloant varchar := '';
	v_co_ventan varchar;
	v_co_venant varchar := '';
	v_ls_bloque varchar := '';
	v_il_insert_cam boolean;
	v_qr_insert_cam varchar;
	v_qr_insert_val varchar;
begin
	/*
		select * from becombas.f_comparar_db_sintaxis(45, 1);
	*/
	-- REVISION DE ESTRUCTURAS
	case p_ti_compar
	when 1 then
		for v_no_esquem in
			select distinct no_esquem
			from becombas.tbtabori
			where id_compar = p_id_compar
			and no_esquem not in ('cpmotdec')
			order by no_esquem
		loop
			if exists (
				select distinct no_esquem from becombas.tbtabdes
				where id_compar = p_id_compar
				and no_esquem = v_no_esquem
			) then
				for v_no_tablas in
					select distinct no_tablas
					from becombas.tbtabori
					where id_compar = p_id_compar
					and no_esquem = v_no_esquem
					order by no_tablas
				loop
					if exists (
						select distinct no_tablas from becombas.tbtabdes
						where id_compar = p_id_compar
						and no_esquem = v_no_esquem
						and no_tablas = v_no_tablas
					) then
						for v_rs_tabori in
							select * from becombas.tbtabori
							where id_compar = p_id_compar
							and no_esquem = v_no_esquem
							and no_tablas = v_no_tablas
							order by nu_ordcol
						loop
							v_qr_return = null;

							select * into v_rs_tabdes
							from becombas.tbtabdes
							where id_compar = p_id_compar
							and no_esquem = v_no_esquem
							and no_tablas = v_no_tablas
							and no_column = v_rs_tabori.no_column;

							if v_rs_tabdes.no_column is null then -- ADD COLUMN
								v_qr_return = 'ALTER TABLE '|| v_no_esquem || '.' || v_no_tablas ||
									' ADD COLUMN '|| v_rs_tabori.no_column ||
									' ' || v_rs_tabori.ti_column ||
									' ' || v_rs_tabori.va_defaul ||
									(case when v_rs_tabori.va_notnul = 'N' then ' NOT NULL' else '' end) ||
									';'
								;
							else
								if v_rs_tabori.ti_column <> v_rs_tabdes.ti_column then -- MODIFY TYPE
									v_qr_return = 'ALTER TABLE '|| v_no_esquem || '.' || v_no_tablas ||
										' ALTER COLUMN '|| v_rs_tabori.no_column ||
										' TYPE ' || v_rs_tabori.ti_column ||
										(case when v_rs_tabori.va_notnul = 'N' then ' NOT NULL' else '' end) ||
										';'
									;
								end if;

								if v_rs_tabori.va_defaul <> v_rs_tabdes.va_defaul then -- MODIFY DEFAULT
									v_qr_return = coalesce(v_qr_return || ' ', '') ||
										'ALTER TABLE '|| v_no_esquem || '.' || v_no_tablas ||
										' ALTER COLUMN '|| v_rs_tabori.no_column ||
										' SET ' || v_rs_tabori.va_defaul ||
										(case when v_rs_tabori.va_notnul = 'N' then ' NOT NULL' else '' end) ||
										';';
								end if;

								if v_rs_tabori.va_notnul <> v_rs_tabdes.va_notnul then -- MODIFY NOT NULL
									v_qr_return = coalesce(v_qr_return || ' ', '') ||
										'ALTER TABLE '|| v_no_esquem || '.' || v_no_tablas ||
										' ALTER COLUMN '|| v_rs_tabori.no_column ||
										(case when v_rs_tabori.va_notnul = 'N' then ' SET NOT NULL' else ' DROP NOT NULL' end) ||
										';'
									;
								end if;
							end if;

							if v_qr_return is not null then
								v_rs_return.va_datori = v_rs_tabori.no_esquem ||'.'|| v_rs_tabori.no_tablas ||'.'|| v_rs_tabori.no_column;
								v_rs_return.va_datdes = v_rs_tabdes.no_esquem ||'.'|| v_rs_tabdes.no_tablas ||'.'|| v_rs_tabdes.no_column;
								v_rs_return.va_sintax = v_qr_return;

								return next v_rs_return;
							end if;
						end loop;	
					else	 -- CREATE TABLE

						if v_qr_return is not null then
							v_rs_return.va_datori = v_rs_tabori.no_esquem ||'.'|| v_rs_tabori.no_tablas;
							v_rs_return.va_datdes = v_rs_tabdes.no_esquem ||'.'|| v_rs_tabdes.no_tablas;
							v_rs_return.va_sintax = v_qr_return;

							return next v_rs_return;
						end if;
					end if;
				end loop;
			else	-- CREATE ESCHEMA

				if v_qr_return is not null then
					v_rs_return.va_datori = v_rs_tabori.no_esquem;
					v_rs_return.va_datdes = v_rs_tabdes.no_esquem;
					v_rs_return.va_sintax = v_qr_return;

					return next v_rs_return;
				end if;
			end if;
		end loop;
		
	when 4 then
		-- REVISION DE FUNCIONES
		for v_rs_funori in
			select *
			from becombas.tbfunori
			where id_compar = p_id_compar
			--and no_esquem not in ('cpmotdec')
			order by no_esquem, no_funcio, ca_parfun
		loop
			v_qr_return = null;

			select * into v_rs_fundes
			from becombas.tbfundes
			where id_compar = p_id_compar
			and no_esquem = v_rs_funori.no_esquem
			and no_funcio = v_rs_funori.no_funcio
			and ca_parfun = v_rs_funori.ca_parfun
			and ls_parfun = v_rs_funori.ls_parfun
			and ls_partyp = v_rs_funori.ls_partyp;

			if v_rs_fundes.no_funcio is null then
				v_qr_return = v_rs_funori.df_funcio;
			elseif (v_rs_funori.ls_parfun <> v_rs_fundes.ls_parfun or 
				v_rs_funori.ls_partyp <> v_rs_fundes.ls_partyp or 
				replace(replace(v_rs_funori.df_funcio, '	', ''), ' ', '') <> replace(replace(v_rs_fundes.df_funcio, '	', ''), ' ', '')
			) then
				v_qr_return = v_rs_funori.df_funcio;
			end if;

			if v_qr_return is not null then
				v_rs_return.va_datori = v_rs_funori.no_esquem || '.' || v_rs_funori.no_funcio ||'('|| rtrim(ltrim(trim(v_rs_funori.ls_parfun), '{'), '}') ||')';
				v_rs_return.va_datdes = v_rs_funori.no_esquem || '.' || v_rs_fundes.no_funcio ||'('|| rtrim(ltrim(trim(v_rs_fundes.ls_parfun), '{'), '}') ||')';
				v_rs_return.va_sintax = v_qr_return || ';';

				return next v_rs_return;
			end if;
		end loop;
	when 2 then
	-- REVISION DE CONTENIDO DE CATALOGOS - INSERT
		for v_rs_catori in 
			select *
			from becombas.tbcatori
			where id_compar = p_id_compar
			and no_esquem|| '.'||no_tablas not in (
				'fwconacc.trusuper',
				'fwconacc.trusumod',
				'cpexpedi.trcondat',
				'fwrecurs.trarcrec',
				'fwgescon.tbbloque',
				'fwgescon.tbblopar',
				'fwgescon.tbblotab',
				'fwgescon.tbventan',
				'fwgescon.tbvenreg',
				'fwgescon.tbventit',
				'fwgescon.tbvenbot',
				'fwgescon.tbbotacc'
			)
			and no_esquem not in (
				'cpparcre',
				'cpmotdec'
			  )
			order by no_esquem, no_tablas
		loop
			v_qr_return = '';
			v_qr_insert = '';
			v_qr_insert_cam = '';
			v_il_insert_cam = true;

			select * into v_rs_catdes
			from becombas.tbcatdes
			where id_compar = p_id_compar
			and no_esquem = v_rs_catori.no_esquem
			and no_tablas = v_rs_catori.no_tablas;

			v_js_conori = v_rs_catori.js_conten::json ->> 'ls_concat';
			v_js_condes = v_rs_catdes.js_conten::json ->> 'ls_concat';

			if v_rs_catdes.no_tablas is not null then
				if v_js_conori::varchar <> v_js_condes::varchar then
				-- TABLA CATALOGO ENCONTRADO Y CON DIFERENCIAS EN SU CONTENIDO
					-- IDENTIFICAMOS LA LLAVE PRIMARIA DE LA TABLA
					select
						tx[1] as co_prike1,
						tx[2] as co_prike2,
						tx[3] as co_prike3,
						tx[4] as co_prike4
					into v_rs_prikey
					from (
						select regexp_split_to_array(tx.no_column, ',')
						from (
							SELECT string_agg(no_column, ',') as no_column
							FROM becombas.tbtabori
							where id_compar = p_id_compar
							and no_esquem = v_rs_catori.no_esquem
							and no_tablas = v_rs_catori.no_tablas
							and va_prikey = 'S'
							group by id_compar, no_esquem, no_tablas
						) tx
					) as dt(tx);

					-- ARMAMOS LA SINTAXIS.
					if v_js_conori is not null then
						for v_js_rowori in
							select *
							from json_array_elements(v_js_conori::json)
						loop
							v_va_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1;
							v_va_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2;
							v_va_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3;
							v_va_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4;

							select tx.js_rowdes into v_js_rowdes
							from (
								select value as js_rowdes
								from json_array_elements(v_js_condes::json)
							) tx
							where (case when v_rs_prikey.co_prike1 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1 else true end)
							and (case when v_rs_prikey.co_prike2 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2 else true end)
							and (case when v_rs_prikey.co_prike3 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3 else true end)
							and (case when v_rs_prikey.co_prike4 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4 else true end)
							;

							if v_js_rowdes is null then
							-- Registro no encontrado, por lo tanto generamos el insert
								v_qr_insert_val = '';

								for v_rs_coltip in
									select no_column, ti_column
									from becombas.tbtabori
									where id_compar = p_id_compar
									and no_esquem = v_rs_catori.no_esquem
									and no_tablas = v_rs_catori.no_tablas
									order by nu_ordcol
								loop
									if v_il_insert_cam then
										v_qr_insert_cam = v_qr_insert_cam || v_rs_coltip.no_column || ',';
									end if;

									v_qr_insert_val = v_qr_insert_val ||  coalesce('''' || (v_js_rowori ->> v_rs_coltip.no_column)::varchar || '''', 'null') || '::' || v_rs_coltip.ti_column || ',';
								end loop;

								if  v_qr_insert_val <> '' then
									v_qr_insert_val = substring(v_qr_insert_val, 1, length(v_qr_insert_val) - 1);

									if v_il_insert_cam then
										v_qr_insert_cam = substring(v_qr_insert_cam, 1, length(v_qr_insert_cam) - 1);
										v_qr_insert = v_qr_insert || 'Insert into ' || v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas ||' (' || v_qr_insert_cam || ')' || chr(10);
										v_qr_insert = v_qr_insert || '	  Select ' || v_qr_insert_val || chr(10);

										v_il_insert_cam = false;
									else
										v_qr_insert = v_qr_insert || 'Union Select ' || v_qr_insert_val || chr(10);
									end if;
								end if;
							end if;
						end loop;

						if v_qr_insert_val <> '' then
							v_qr_return = v_qr_return || v_qr_insert || ';';
						end if;
					end if;
				end if;
			else
			-- NUEVA TABLA CATALOGO
				for v_js_rowori in
					select *
					from json_array_elements(v_js_conori::json)
				loop
					v_qr_insert_val = null;

					for v_rs_coltip in
						select no_column, ti_column
						from becombas.tbtabori
						where id_compar = p_id_compar
						and no_esquem = v_rs_catori.no_esquem
						and no_tablas = v_rs_catori.no_tablas
						order by nu_ordcol
					loop
						if v_il_insert_cam then
							v_qr_insert_cam = v_qr_insert_cam || v_rs_coltip.no_column || ', ';
						end if;

						v_qr_insert_val = v_qr_insert_val || coalesce('''' || (v_js_rowori ->> v_rs_coltip.no_column)::varchar || '''', 'null') || '::' || v_rs_coltip.ti_column || ', ';
					end loop;

					if  v_qr_insert_val <> '' then
						v_qr_insert_val = substring(v_qr_insert_val, 1, length(v_qr_insert_val) - 1);

						if v_il_insert_cam then
							v_qr_insert_cam = substring(v_qr_insert_cam, 1, length(v_qr_insert_cam) - 1);

							v_qr_return = v_qr_return || 'Insert ' || v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas ||' (' || v_qr_insert_cam || ')' || chr(10);
							v_qr_return = v_qr_return || '	  Select ' || v_qr_insert_val || chr(10);

							v_il_insert_cam = false;
						else
							v_qr_return = v_qr_return || 'Union Select ' || v_qr_insert_val || chr(10);
						end if;
					end if;
				end loop;

				v_qr_return = v_qr_return || ';' ||chr(10);
			end if;

			if v_qr_return <> '' then
				v_rs_return.va_datori = v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas;
				v_rs_return.va_datdes = v_rs_catdes.no_esquem || '.' || v_rs_catdes.no_tablas;
				v_rs_return.va_sintax = v_qr_return;

				return next v_rs_return;
			end if;
		end loop;
	when 3 then
	-- REVISION DE CONTENIDO DE CATALOGOS - UPDATE
		for v_rs_catori in 
			select *
			from becombas.tbcatori
			where id_compar = p_id_compar
			and no_esquem|| '.'||no_tablas not in (
				'fwconacc.trusuper',
				'fwconacc.trusumod',
				'cpexpedi.trcondat',
				'fwrecurs.trarcrec'
			)
			and no_esquem not in (
				'cpmotdec',
				'cpparcre'
			)
			order by no_esquem, no_tablas
		loop
			v_qr_return = '';
			v_qr_insert = '';
			v_qr_insert_cam = '';
			v_il_insert_cam = true;

			select * into v_rs_catdes
			from becombas.tbcatdes
			where id_compar = p_id_compar
			and no_esquem = v_rs_catori.no_esquem
			and no_tablas = v_rs_catori.no_tablas;

			v_js_conori = v_rs_catori.js_conten::json ->> 'ls_concat';
			v_js_condes = v_rs_catdes.js_conten::json ->> 'ls_concat';

			if v_rs_catdes.no_tablas is not null then
				if v_js_conori::varchar <> v_js_condes::varchar then
				-- TABLA CATALOGO ENCONTRADO Y CON DIFERENCIAS EN SU CONTENIDO
					-- IDENTIFICAMOS LA LLAVE PRIMARIA DE LA TABLA
					select
						tx[1] as co_prike1,
						tx[2] as co_prike2,
						tx[3] as co_prike3,
						tx[4] as co_prike4
					into v_rs_prikey
					from (
						select regexp_split_to_array(tx.no_column, ',')
						from (
							SELECT string_agg(no_column, ',') as no_column
							FROM becombas.tbtabori
							where id_compar = p_id_compar
							and no_esquem = v_rs_catori.no_esquem
							and no_tablas = v_rs_catori.no_tablas
							and va_prikey = 'S'
							group by id_compar, no_esquem, no_tablas
						) tx
					) as dt(tx);

					-- ARMAMOS LA SINTAXIS.
					if v_js_conori is not null then
						for v_js_rowori in
							select *
							from json_array_elements(v_js_conori::json)
						loop
							v_va_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1;
							v_va_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2;
							v_va_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3;
							v_va_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4;

							select tx.js_rowdes into v_js_rowdes
							from (
								select value as js_rowdes
								from json_array_elements(v_js_condes::json)
							) tx
							where (case when v_rs_prikey.co_prike1 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1 else true end)
							and (case when v_rs_prikey.co_prike2 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2 else true end)
							and (case when v_rs_prikey.co_prike3 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3 else true end)
							and (case when v_rs_prikey.co_prike4 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4 else true end)
							;

							if v_js_rowdes is not null then
							-- registro encontrado
								if v_js_rowori::varchar <> v_js_rowdes::varchar
									and v_rs_catori.no_esquem ||'.'||v_rs_catori.no_tablas not in (
										'cpexpedi.trentdatvar',
										'cpexpedi.trentpro',
										'cpexpedi.trproarc',
										'cpgendoc.tcgendoc',
										'cpmotdec.tcregpolvar',
										'cpmotdec.trregpol',
										'cpparcre.thbonfam',
										'cpparcre.thcarveh'
									)
								then
								-- registros diferentes
									-- generamos el update
									v_qr_actcam = '';

									for v_rs_coltip in
										select no_column, ti_column
										from becombas.tbtabori
										where id_compar = p_id_compar
										and no_esquem = v_rs_catori.no_esquem
										and no_tablas = v_rs_catori.no_tablas
										and va_prikey <> 'S'
										and no_column not ilike 'fe_%'
										and no_column <> 'us_regist'
										and no_column <> 'co_archiv'
										order by nu_ordcol
									loop
										if ((v_js_rowori ->> v_rs_coltip.no_column)::varchar <> (v_js_rowdes ->> v_rs_coltip.no_column)::varchar) or (v_js_rowdes ->> v_rs_coltip.no_column)::varchar is null then
											v_qr_actcam = v_qr_actcam || 
												v_rs_coltip.no_column || ' = ''' || (v_js_rowori ->> v_rs_coltip.no_column)::varchar || '''::' || v_rs_coltip.ti_column || ',';
										end if;
									end loop;

									if v_qr_actcam <> '' then
										v_qr_actcam = substring(v_qr_actcam, 1, length(v_qr_actcam) - 1);

										v_qr_return = v_qr_return || 'Update ' || v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas || chr(10) ||
											' Set ' || coalesce(v_qr_actcam, 'v_qr_actcam salio null') || chr(10);

										for v_rs_campos in
											select no_column, ti_column, nu_ordcol
											from becombas.tbtabori
											where id_compar = p_id_compar
											and no_esquem = v_rs_catori.no_esquem
											and no_tablas = v_rs_catori.no_tablas
											and va_prikey = 'S'
											order by nu_ordcol
										loop
											v_va_prikey = v_js_rowori ->> v_rs_campos.no_column;

											if v_rs_campos.nu_ordcol = 1 then
												v_qr_return = v_qr_return || 'where ' || (case when v_rs_campos.no_column is not null then v_rs_campos.no_column || ' = ''' || v_va_prikey || '''::' || v_rs_campos.ti_column else ' false' end);
											else
												v_qr_return = v_qr_return || (case when v_rs_campos.no_column is not null then ' and ' || v_rs_campos.no_column || ' = ''' || v_va_prikey || '''::' || v_rs_campos.ti_column else '' end);
											end if;
										end loop;

										v_qr_return = v_qr_return || ';' || chr(10) || chr(10);
									end if;
								end if;
							end if;
						end loop;
						
						if v_qr_insert_val <> '' then
							v_qr_return = v_qr_return || v_qr_insert || ';';
						end if;
					end if;
				end if;
			end if;

			if v_qr_return <> '' then
				v_rs_return.va_datori = v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas;
				v_rs_return.va_datdes = v_rs_catdes.no_esquem || '.' || v_rs_catdes.no_tablas;
				v_rs_return.va_sintax = v_qr_return;
				
				return next v_rs_return;
			end if;
		end loop;
	when 5 then
	-- REVISION DE CONTENIDO DE CATALOGOS - INSERT
		for v_rs_catori in 
			select *
			from becombas.tbcatori
			where id_compar = p_id_compar
			and no_esquem|| '.'||no_tablas in (
				'fwgescon.tbbloque',  -- bloque
				'fwgescon.tbblopar',  -- bloque
				'fwgescon.tbblotab',  -- bloque
				'fwgescon.tbventan',  -- bloque
				'fwgescon.tbvenreg',  -- bloque
				'fwgescon.tbventit',  -- ventan
				'fwgescon.tbvenbot',  -- bloque
				'fwgescon.tbbotacc'   -- ventan
			)
			order by no_esquem, no_tablas
		loop
			v_qr_return = '';
			v_qr_insert = '';
			v_qr_insert_cam = '';
			v_il_insert_cam = true;

			select * into v_rs_catdes
			from becombas.tbcatdes
			where id_compar = p_id_compar
			and no_esquem = v_rs_catori.no_esquem
			and no_tablas = v_rs_catori.no_tablas;

			v_js_conori = v_rs_catori.js_conten::json ->> 'ls_concat';
			v_js_condes = v_rs_catdes.js_conten::json ->> 'ls_concat';

			if v_rs_catdes.no_tablas is not null then
				if v_js_conori::varchar <> v_js_condes::varchar then
				-- TABLA CATALOGO ENCONTRADO Y CON DIFERENCIAS EN SU CONTENIDO
					-- IDENTIFICAMOS LA LLAVE PRIMARIA DE LA TABLA
					select
						tx[1] as co_prike1,
						tx[2] as co_prike2,
						tx[3] as co_prike3,
						tx[4] as co_prike4
					into v_rs_prikey
					from (
						select regexp_split_to_array(tx.no_column, ',')
						from (
							SELECT string_agg(no_column, ',') as no_column
							FROM becombas.tbtabori
							where id_compar = p_id_compar
							and no_esquem = v_rs_catori.no_esquem
							and no_tablas = v_rs_catori.no_tablas
							and va_prikey = 'S'
							group by id_compar, no_esquem, no_tablas
						) tx
					) as dt(tx);

					-- ARMAMOS LA SINTAXIS.
					if v_js_conori is not null then
						for v_js_rowori in
							select * from json_array_elements(v_js_conori::json)
						loop
							v_va_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1;
							v_va_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2;
							v_va_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3;
							v_va_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4;
							
							select tx.js_rowdes into v_js_rowdes
							from (
								select value as js_rowdes
								from json_array_elements(v_js_condes::json)
							) tx
							where (case when v_rs_prikey.co_prike1 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1 else true end)
							and (case when v_rs_prikey.co_prike2 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2 else true end)
							and (case when v_rs_prikey.co_prike3 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3 else true end)
							and (case when v_rs_prikey.co_prike4 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4 else true end)
							;
							
							if v_js_rowdes is null then
							-- Registro no encontrado, por lo tanto generamos el insert
								v_qr_insert_val = '';

								for v_rs_coltip in
									select no_column, ti_column
									from becombas.tbtabori
									where id_compar = p_id_compar
									and no_esquem = v_rs_catori.no_esquem
									and no_tablas = v_rs_catori.no_tablas
									order by nu_ordcol
								loop
									if v_rs_catori.no_tablas in ('tbbloque', 'tbblopar', 'tbblotab', 'tbventan', 'tbvenreg', 'tbvenbot') then
										if v_rs_coltip.no_column = 'co_bloque' then
											v_co_bloque = (v_js_rowori ->> v_rs_coltip.no_column)::varchar;
											
											if v_co_bloque <> v_co_bloant then
												v_co_bloant = v_co_bloque;
												
												v_ls_bloque = v_ls_bloque || ' union select ' || v_co_bloque;
											end if;
										end if;
									elseif v_rs_catori.no_tablas in ('tbventit', 'tbbotacc') then
										if v_rs_coltip.no_column = 'co_ventan' then
											v_co_ventan = (v_js_rowori ->> v_rs_coltip.no_column)::varchar;
											
											if v_co_ventan <> v_co_venant then
												v_co_venant = v_co_ventan;
												
												v_ls_bloque = v_ls_bloque || ' union select (select co_bloque from fwgescon.tbventan where co_ventan = ' || v_co_ventan || ')';
											end if;
										end if;
									end if;				
								end loop;
								
								if v_ls_bloque <> '' then
									v_qr_return = v_qr_return || v_ls_bloque || chr(10);
									v_ls_bloque = '';
								end if;
							end if;
						end loop;
					end if;
				end if;
			end if;

			if v_qr_return <> '' then
				v_rs_return.va_datori = v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas;
				v_rs_return.va_datdes = v_rs_catdes.no_esquem || '.' || v_rs_catdes.no_tablas;
				v_rs_return.va_sintax = v_qr_return;

				return next v_rs_return;
			end if;
		end loop;
	when 6 then
	-- REVISION DE CONTENIDO DE CATALOGOS - UPDATE
		for v_rs_catori in 
			select *
			from becombas.tbcatori
			where id_compar = p_id_compar
			and no_esquem|| '.'||no_tablas in (
				'fwgescon.tbbloque',
				'fwgescon.tbblopar',
				'fwgescon.tbblotab',
				'fwgescon.tbventan',
				'fwgescon.tbvenreg',
				'fwgescon.tbventit',
				'fwgescon.tbvenbot',
				'fwgescon.tbbotacc'
			)
			order by no_esquem, no_tablas
		loop
			v_qr_return = '';
			v_qr_insert = '';
			v_qr_insert_cam = '';
			v_il_insert_cam = true;

			select * into v_rs_catdes
			from becombas.tbcatdes
			where id_compar = p_id_compar
			and no_esquem = v_rs_catori.no_esquem
			and no_tablas = v_rs_catori.no_tablas;

			v_js_conori = v_rs_catori.js_conten::json ->> 'ls_concat';
			v_js_condes = v_rs_catdes.js_conten::json ->> 'ls_concat';

			if v_rs_catdes.no_tablas is not null then
				if v_js_conori::varchar <> v_js_condes::varchar then
				-- TABLA CATALOGO ENCONTRADO Y CON DIFERENCIAS EN SU CONTENIDO
					-- IDENTIFICAMOS LA LLAVE PRIMARIA DE LA TABLA
					select
						tx[1] as co_prike1,
						tx[2] as co_prike2,
						tx[3] as co_prike3,
						tx[4] as co_prike4
					into v_rs_prikey
					from (
						select regexp_split_to_array(tx.no_column, ',')
						from (
							SELECT string_agg(no_column, ',') as no_column
							FROM becombas.tbtabori
							where id_compar = p_id_compar
							and no_esquem = v_rs_catori.no_esquem
							and no_tablas = v_rs_catori.no_tablas
							and va_prikey = 'S'
							group by id_compar, no_esquem, no_tablas
						) tx
					) as dt(tx);

					-- ARMAMOS LA SINTAXIS.
					if v_js_conori is not null then
						for v_js_rowori in
							select *
							from json_array_elements(v_js_conori::json)
						loop
							v_va_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1;
							v_va_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2;
							v_va_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3;
							v_va_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4;

							select tx.js_rowdes into v_js_rowdes
							from (
								select value as js_rowdes
								from json_array_elements(v_js_condes::json)
							) tx
							where (case when v_rs_prikey.co_prike1 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1 else true end)
							and (case when v_rs_prikey.co_prike2 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2 else true end)
							and (case when v_rs_prikey.co_prike3 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3 else true end)
							and (case when v_rs_prikey.co_prike4 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4 else true end)
							;

							if v_js_rowdes is not null then
							-- registro encontrado
								if v_js_rowori::varchar <> v_js_rowdes::varchar
									and v_rs_catori.no_esquem ||'.'||v_rs_catori.no_tablas not in (
										'cpexpedi.trentdatvar',
										'cpexpedi.trentpro',
										'cpexpedi.trproarc',
										'cpgendoc.tcgendoc',
										'cpmotdec.tcregpolvar',
										'cpmotdec.trregpol',
										'cpparcre.thbonfam',
										'cpparcre.thcarveh'
									)
								then
								-- registros diferentes
									-- generamos el update
									v_qr_actcam = '';

									for v_rs_coltip in
										select no_column, ti_column
										from becombas.tbtabori
										where id_compar = p_id_compar
										and no_esquem = v_rs_catori.no_esquem
										and no_tablas = v_rs_catori.no_tablas
										and va_prikey <> 'S'
										and no_column not ilike 'fe_%'
										and no_column <> 'us_regist'
										and no_column <> 'co_archiv'
										order by nu_ordcol
									loop
										if (v_js_rowori ->> v_rs_coltip.no_column)::varchar <> (v_js_rowdes ->> v_rs_coltip.no_column)::varchar then
											v_qr_actcam = v_qr_actcam || 
												v_rs_coltip.no_column || ' = ''' || (v_js_rowori ->> v_rs_coltip.no_column)::varchar || '''::' || v_rs_coltip.ti_column || ',';
										end if;
									end loop;

									if v_qr_actcam <> '' then
										v_qr_actcam = substring(v_qr_actcam, 1, length(v_qr_actcam) - 1);

										v_qr_return = v_qr_return || 'Update ' || v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas || chr(10) ||
											' Set ' || coalesce(v_qr_actcam, 'v_qr_actcam salio null') || chr(10);

										for v_rs_campos in
											select no_column, ti_column, nu_ordcol
											from becombas.tbtabori
											where id_compar = p_id_compar
											and no_esquem = v_rs_catori.no_esquem
											and no_tablas = v_rs_catori.no_tablas
											and va_prikey = 'S'
											order by nu_ordcol
										loop
											v_va_prikey = v_js_rowori ->> v_rs_campos.no_column;

											if v_rs_campos.nu_ordcol = 1 then
												v_qr_return = v_qr_return || 'where ' || (case when v_rs_campos.no_column is not null then v_rs_campos.no_column || ' = ''' || v_va_prikey || '''::' || v_rs_campos.ti_column else ' false' end);
											else
												v_qr_return = v_qr_return || (case when v_rs_campos.no_column is not null then ' and ' || v_rs_campos.no_column || ' = ''' || v_va_prikey || '''::' || v_rs_campos.ti_column else '' end);
											end if;
										end loop;

										v_qr_return = v_qr_return || ';' || chr(10) || chr(10);
									end if;
								end if;
							end if;
						end loop;
						
						if v_qr_insert_val <> '' then
							v_qr_return = v_qr_return || v_qr_insert || ';';
						end if;
					end if;
				end if;
			end if;

			if v_qr_return <> '' then
				v_rs_return.va_datori = v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas;
				v_rs_return.va_datdes = v_rs_catdes.no_esquem || '.' || v_rs_catdes.no_tablas;
				v_rs_return.va_sintax = v_qr_return;
				
				return next v_rs_return;
			end if;
		end loop;
	end case;
	
	return;
end;
$$;


ALTER FUNCTION becombas.f_comparar_db_sintaxis(p_id_compar integer, p_ti_compar integer) OWNER TO aguzman;

--
-- Name: f_comparar_db_sintaxis_db_central_v1(integer, integer); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_comparar_db_sintaxis_db_central_v1(p_id_compar integer, p_ti_compar integer) RETURNS SETOF becombas.pdcompar2
    LANGUAGE plpgsql
    AS $$
declare
	v_rs_tabori record;
	v_rs_tabdes record;
	v_rs_funori record;
	v_rs_fundes record;
	v_rs_catori record;
	v_rs_catdes record;
	v_rs_prikey record;
	v_rs_coltip record;
	v_rs_campos record;
	v_rs_return becombas.pdcompar2%rowtype;
	v_js_conori json;
	v_js_condes json;
	v_js_rowori json;
	v_js_rowdes json;
	v_no_esquem varchar;
	v_no_tablas varchar;
	v_qr_return varchar;
	v_va_prike1 varchar;
	v_va_prike2 varchar;
	v_va_prike3 varchar;
	v_va_prike4 varchar;
	v_qr_insert varchar;
	v_qr_actcam varchar;
	v_va_prikey varchar;
	v_qr_prikey varchar;
	v_il_insert_cam boolean;
	v_qr_insert_cam varchar;
	v_qr_insert_val varchar;
begin
	/*
		select * from becombas.f_comparar_db_sintaxis_db_central_v1(45, 1);
	*/
	-- REVISION DE ESTRUCTURAS
	case p_ti_compar
	when 1 then
		for v_no_esquem in
			select distinct no_esquem
			from becombas.tbtabori
			where id_compar = p_id_compar
			and no_esquem not in ('becombas')
			order by no_esquem
		loop
			if exists (
				select distinct no_esquem from becombas.tbtabdes
				where id_compar = p_id_compar
				and no_esquem = v_no_esquem
			) then
				for v_no_tablas in
					select distinct no_tablas
					from becombas.tbtabori
					where id_compar = p_id_compar
					and no_esquem = v_no_esquem
					order by no_tablas
				loop
					if exists (
						select distinct no_tablas
						from becombas.tbtabdes
						where id_compar = p_id_compar
						and no_esquem = v_no_esquem
						and no_tablas = v_no_tablas
					) then
						for v_rs_tabori in
							select * from becombas.tbtabori
							where id_compar = p_id_compar
							and no_esquem = v_no_esquem
							and no_tablas = v_no_tablas
							order by nu_ordcol
						loop
							v_qr_return = null;

							select * into v_rs_tabdes
							from becombas.tbtabdes
							where id_compar = p_id_compar
							and no_esquem = v_no_esquem
							and no_tablas = v_no_tablas
							and no_column = v_rs_tabori.no_column;

							if v_rs_tabdes.no_column is null then -- ADD COLUMN
								v_qr_return = 'ALTER TABLE IF EXISTS '|| v_no_esquem || '.' || v_no_tablas ||
									' ADD COLUMN '|| v_rs_tabori.no_column ||
									' ' || v_rs_tabori.ti_column ||
									' ' || v_rs_tabori.va_defaul ||
									(case when v_rs_tabori.va_notnul = 'N' then ' NOT NULL' else '' end) ||
									';'
								;
							else
								if v_rs_tabori.ti_column <> v_rs_tabdes.ti_column then -- MODIFY TYPE
									v_qr_return = 'ALTER TABLE IF EXISTS '|| v_no_esquem || '.' || v_no_tablas ||
										' ALTER COLUMN '|| v_rs_tabori.no_column ||
										' TYPE ' || v_rs_tabori.ti_column ||
										';'
									;
								end if;

								if v_rs_tabori.va_defaul <> v_rs_tabdes.va_defaul then -- MODIFY DEFAULT
									v_qr_return = coalesce(v_qr_return || ' ', '') ||
										'ALTER TABLE IF EXISTS '|| v_no_esquem || '.' || v_no_tablas ||
										' ALTER COLUMN '|| v_rs_tabori.no_column ||
										(case when v_rs_tabori.va_defaul <> '' then ' SET ' || v_rs_tabori.va_defaul else ' DROP DEFAULT' end) || 
										';';
								end if;

								if v_rs_tabori.va_notnul <> v_rs_tabdes.va_notnul then -- MODIFY NOT NULL
									v_qr_return = coalesce(v_qr_return || ' ', '') ||
										'ALTER TABLE IF EXISTS '|| v_no_esquem || '.' || v_no_tablas ||
										' ALTER COLUMN '|| v_rs_tabori.no_column ||
										(case when v_rs_tabori.va_notnul = 'N' then ' SET NOT NULL' else ' DROP NOT NULL' end) ||
										';'
									;
								end if;
							end if;

							if v_qr_return is not null then
								v_rs_return.va_datori = v_rs_tabori.no_esquem ||'.'|| v_rs_tabori.no_tablas ||'.'|| v_rs_tabori.no_column;
								v_rs_return.va_datdes = v_rs_tabdes.no_esquem ||'.'|| v_rs_tabdes.no_tablas ||'.'|| v_rs_tabdes.no_column;
								v_rs_return.va_sintax = v_qr_return;

								return next v_rs_return;
							end if;
							
							if v_rs_tabori.va_forkey <> v_rs_tabdes.va_forkey then -- MODIFY FOREY_KEY
								v_rs_return.va_datori = v_rs_tabori.no_esquem ||'.'|| v_rs_tabori.no_tablas ||'.'|| v_rs_tabori.no_column;
								v_rs_return.va_datdes = v_rs_tabdes.no_esquem ||'.'|| v_rs_tabdes.no_tablas ||'.'|| v_rs_tabdes.no_column;
								v_rs_return.va_sintax = 'Revisar foreing key para este campo';

								return next v_rs_return;
							end if;
							
						end loop;	
					else	 -- CREATE TABLE
						v_qr_return := 'CREATE TABLE IF NOT EXISTS '|| v_no_esquem ||'.'|| v_no_tablas ||' (';
						
						v_qr_prikey := null;
						
						for v_rs_tabori in
							select * from becombas.tbtabori
							where id_compar = p_id_compar
							
							and no_esquem = v_no_esquem
							and no_tablas = v_no_tablas
							order by nu_ordcol
						loop
							v_qr_return := v_qr_return || chr(10) ||
								'	' || v_rs_tabori.no_column ||
								' ' || v_rs_tabori.ti_column ||
								(case when nullif(v_rs_tabori.va_defaul, '') is not null then ' ' || v_rs_tabori.va_defaul else '' end) ||
								(case when v_rs_tabori.va_notnul = 'N' then ' NOT NULL' else '' end);
								
								v_qr_return := v_qr_return || ',';
								
								if v_rs_tabori.va_prikey = 'S' then
									v_qr_prikey := coalesce(v_qr_prikey || ', ', '') || v_rs_tabori.no_column;
								end if;
						end loop;
						
						if v_qr_prikey is not null then
							v_qr_prikey := 'CONSTRAINT '|| v_no_tablas ||'_pkey PRIMARY KEY (' || v_qr_prikey || ')';
						end if;
						
						if v_qr_return is not null then
							v_rs_return.va_datori = v_no_esquem ||'.'|| v_no_tablas;
							v_rs_return.va_datdes = null;
							v_rs_return.va_sintax = v_qr_return || chr(10) || coalesce(v_qr_prikey || chr(10), '') || ');';

							return next v_rs_return;
						end if;
					end if;
				end loop;
			else	-- CREATE ESCHEMA
				v_qr_return := 'CREATE SCHEMA IF NOT EXISTS'|| v_no_esquem;
				
				if v_qr_return is not null then
					v_rs_return.va_datori = v_no_esquem;
					v_rs_return.va_datdes = v_no_esquem;
					v_rs_return.va_sintax = v_qr_return;

					return next v_rs_return;
				end if;
			end if;
		end loop;
	when 4 then
	-- REVISION DE CONTENIDO DE CATALOGOS - INSERT
		for v_rs_catori in 
			select *
			from becombas.tbcatori
			where id_compar = p_id_compar
			and no_esquem|| '.'||no_tablas not in (
				'fwconacc.trusuper',
				'fwconacc.trusumod',
				'fwconacc.tcperfil',
				'fwconacc.trpermod',
				'fwconacc.tbmensis',
				'fwconacc.tbmodulo',
				'cpgendoc.tcgendoc',
				'cpgendoc.tcparame',
				'cpgendoc.tctablas',
				'cpgendoc.tcvariab',
				'fwconacc.trmodblo',
				'cpexpedi.trcondat',
				'fwrecurs.trarcrec',
				'cppublic.trperage',
				'cppublic.tctartas',
				'cppublic.tcraiexc',
				'fwgescon.tbbloque',
				'fwgescon.tbblopar',
				'fwgescon.tbblotab',
				'fwgescon.tcregist',
				'fwgescon.tbventan',
				'fwgescon.tbvenreg',
				'fwgescon.tbventit',
				'fwgescon.tbvenbot',
				'fwgescon.tbbotacc'
			)
			and no_esquem not in (
				'cpparcre',
				'cpmotdec'
			  )
			order by no_esquem, no_tablas
		loop
			v_qr_return = '';
			v_qr_insert = '';
			v_qr_insert_cam = '';
			v_il_insert_cam = true;

			select * into v_rs_catdes
			from becombas.tbcatdes
			where id_compar = p_id_compar
			and no_esquem = v_rs_catori.no_esquem
			and no_tablas = v_rs_catori.no_tablas;

			v_js_conori = v_rs_catori.js_conten::json ->> 'ls_concat';
			v_js_condes = v_rs_catdes.js_conten::json ->> 'ls_concat';

			if v_rs_catdes.no_tablas is not null then
				if v_js_conori::varchar <> v_js_condes::varchar then
				-- TABLA CATALOGO ENCONTRADO Y CON DIFERENCIAS EN SU CONTENIDO
					-- IDENTIFICAMOS LA LLAVE PRIMARIA DE LA TABLA
					select
						tx[1] as co_prike1,
						tx[2] as co_prike2,
						tx[3] as co_prike3,
						tx[4] as co_prike4
					into v_rs_prikey
					from (
						select regexp_split_to_array(tx.no_column, ',')
						from (
							SELECT string_agg(no_column, ',') as no_column
							FROM becombas.tbtabori
							where id_compar = p_id_compar
							and no_esquem = v_rs_catori.no_esquem
							and no_tablas = v_rs_catori.no_tablas
							and va_prikey = 'S'
							group by id_compar, no_esquem, no_tablas
						) tx
					) as dt(tx);

					-- ARMAMOS LA SINTAXIS.
					if v_js_conori is not null then
						for v_js_rowori in
							select *
							from json_array_elements(v_js_conori::json)
						loop
							v_va_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1;
							v_va_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2;
							v_va_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3;
							v_va_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4;

							select tx.js_rowdes into v_js_rowdes
							from (
								select value as js_rowdes
								from json_array_elements(v_js_condes::json)
							) tx
							where (case when v_rs_prikey.co_prike1 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1 else true end)
							and (case when v_rs_prikey.co_prike2 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2 else true end)
							and (case when v_rs_prikey.co_prike3 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3 else true end)
							and (case when v_rs_prikey.co_prike4 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4 else true end)
							;

							if v_js_rowdes is null then
							-- Registro no encontrado, por lo tanto generamos el insert
								v_qr_insert_val = '';

								for v_rs_coltip in
									select no_column, ti_column
									from becombas.tbtabori
									where id_compar = p_id_compar
									and no_esquem = v_rs_catori.no_esquem
									and no_tablas = v_rs_catori.no_tablas
									order by nu_ordcol
								loop
									if v_il_insert_cam then
										v_qr_insert_cam = v_qr_insert_cam || v_rs_coltip.no_column || ',';
									end if;

									v_qr_insert_val = v_qr_insert_val ||  coalesce('''' || replace((v_js_rowori ->> v_rs_coltip.no_column)::varchar, '''', '''''') || '''', 'null') || '::' || v_rs_coltip.ti_column || ',';
								end loop;

								if  v_qr_insert_val <> '' then
									v_qr_insert_val = substring(v_qr_insert_val, 1, length(v_qr_insert_val) - 1);

									if v_il_insert_cam then
										v_qr_insert_cam = substring(v_qr_insert_cam, 1, length(v_qr_insert_cam) - 1);
										v_qr_insert = v_qr_insert || 'Insert into ' || v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas ||' (' || v_qr_insert_cam || ')' || chr(10);
										v_qr_insert = v_qr_insert || '	  Select ' || v_qr_insert_val || chr(10);

										v_il_insert_cam = false;
									else
										v_qr_insert = v_qr_insert || 'Union Select ' || v_qr_insert_val || chr(10);
									end if;
								end if;
							end if;
						end loop;

						if v_qr_insert_val <> '' then
							v_qr_return = v_qr_return || v_qr_insert || ';';
						end if;
					end if;
				end if;
			else
			-- NUEVA TABLA CATALOGO
				for v_js_rowori in
					select *
					from json_array_elements(v_js_conori::json)
				loop
					v_qr_insert_val = '';
					
					for v_rs_coltip in
						select no_column, ti_column
						from becombas.tbtabori
						where id_compar = p_id_compar
						and no_esquem = v_rs_catori.no_esquem
						and no_tablas = v_rs_catori.no_tablas
						order by nu_ordcol
					loop
						if v_il_insert_cam then
							v_qr_insert_cam = v_qr_insert_cam || v_rs_coltip.no_column || ', ';
						end if;
												
						v_qr_insert_val = v_qr_insert_val || 
							coalesce('''' || (v_js_rowori ->> v_rs_coltip.no_column)::varchar || '''', 'null') || '::' || v_rs_coltip.ti_column || ', '
						;
					end loop;
					
					if  v_qr_insert_val <> '' then
						v_qr_insert_val = substring(v_qr_insert_val, 1, length(v_qr_insert_val) - 1);

						if v_il_insert_cam then
							v_qr_insert_cam = substring(v_qr_insert_cam, 1, length(v_qr_insert_cam) - 1);

							v_qr_return = v_qr_return || 'Insert ' || v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas ||' (' || v_qr_insert_cam || ')' || chr(10);
							v_qr_return = v_qr_return || '	  Select ' || v_qr_insert_val || chr(10);
						else
							v_qr_return = v_qr_return || 'Union Select ' || v_qr_insert_val || chr(10);
						end if;
					end if;
					
					v_il_insert_cam = false;
				end loop;
				
				v_qr_return = v_qr_return || ';' ||chr(10);
			end if;

			if v_qr_return <> '' then
				v_rs_return.va_datori = v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas;
				v_rs_return.va_datdes = v_rs_catdes.no_esquem || '.' || v_rs_catdes.no_tablas;
				v_rs_return.va_sintax = v_qr_return;

				return next v_rs_return;
			end if;
		end loop;
	when 5 then
	-- REVISION DE CONTENIDO DE CATALOGOS - UPDATE WF
		for v_rs_catori in 
			select *
			from becombas.tbcatori
			where id_compar = p_id_compar
			and no_esquem || '.' || no_tablas not in (
				'fwconacc.trusuper',
				'fwconacc.trusumod',
				'fwconacc.tcperfil',
				'fwconacc.trpermod',
				'fwconacc.tbmensis',
				'fwconacc.tbmodulo',
				'cpgendoc.tcgendoc',
				'cpgendoc.tcparame',
				'cpgendoc.tctablas',
				'cpgendoc.tcvariab',
				'cppublic.trperage',
				'cppublic.tctartas',
				'fwconacc.trmodblo',
				'cpexpedi.trcondat',
				'cpexpedi.trentdatvar',
				'cpexpedi.trentpro',
				'cpexpedi.trproarc',
				'fwrecurs.trarcrec',
				'fwgescon.tbbloque',
				'fwgescon.tbblopar',
				'fwgescon.tbblotab',
				'fwgescon.tbventan',
				'fwgescon.tbvenreg',
				'fwgescon.tbventit',
				'fwgescon.tbvenbot',
				'fwgescon.tbbotacc'
			)
			and no_esquem not in (
				'cpmotdec',
				'cpparcre'
			)
			order by no_esquem, no_tablas
		loop
			v_qr_return = '';
			v_qr_insert = '';
			v_qr_insert_cam = '';
			v_il_insert_cam = true;

			select * into v_rs_catdes
			from becombas.tbcatdes
			where id_compar = p_id_compar
			and no_esquem = v_rs_catori.no_esquem
			and no_tablas = v_rs_catori.no_tablas;
			
			v_js_conori = v_rs_catori.js_conten::json ->> 'ls_concat';
			v_js_condes = v_rs_catdes.js_conten::json ->> 'ls_concat';

			if v_rs_catdes.no_tablas is not null then
				if v_js_conori::varchar <> v_js_condes::varchar then
				-- TABLA CATALOGO ENCONTRADO Y CON DIFERENCIAS EN SU CONTENIDO
					-- IDENTIFICAMOS LA LLAVE PRIMARIA DE LA TABLA
					select
						tx[1] as co_prike1,
						tx[2] as co_prike2,
						tx[3] as co_prike3,
						tx[4] as co_prike4
					into v_rs_prikey
					from (
						select regexp_split_to_array(tx.no_column, ',')
						from (
							SELECT string_agg(no_column, ',') as no_column
							FROM becombas.tbtabori
							where id_compar = p_id_compar
							and no_esquem = v_rs_catori.no_esquem
							and no_tablas = v_rs_catori.no_tablas
							and va_prikey = 'S'
							group by id_compar, no_esquem, no_tablas
						) tx
					) as dt(tx);

					-- ARMAMOS LA SINTAXIS.
					if v_js_conori is not null then
						for v_js_rowori in
							select *
							from json_array_elements(v_js_conori::json)
						loop
							v_va_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1;
							v_va_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2;
							v_va_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3;
							v_va_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4;

							select tx.js_rowdes into v_js_rowdes
							from (
								select value as js_rowdes
								from json_array_elements(v_js_condes::json)
							) tx
							where (case when v_rs_prikey.co_prike1 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1 else true end)
							and (case when v_rs_prikey.co_prike2 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2 else true end)
							and (case when v_rs_prikey.co_prike3 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3 else true end)
							and (case when v_rs_prikey.co_prike4 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4 else true end)
							;

							if v_js_rowdes is not null then
							-- registro encontrado
								if v_js_rowori::varchar <> v_js_rowdes::varchar then
								-- registros diferentes
									-- generamos el update
									v_qr_actcam = '';

									for v_rs_coltip in
										select no_column, ti_column
										from becombas.tbtabori
										where id_compar = p_id_compar
										and no_esquem = v_rs_catori.no_esquem
										and no_tablas = v_rs_catori.no_tablas
										and va_prikey <> 'S'
										and no_column not ilike 'fe_%'
										and no_column <> 'us_regist'
										and no_column <> 'co_archiv'
										order by nu_ordcol
									loop
										if ((v_js_rowori ->> v_rs_coltip.no_column)::varchar <> (v_js_rowdes ->> v_rs_coltip.no_column)::varchar) or (v_js_rowdes ->> v_rs_coltip.no_column)::varchar is null then
											v_qr_actcam = v_qr_actcam || 
												v_rs_coltip.no_column || ' = ''' || replace((v_js_rowori ->> v_rs_coltip.no_column)::varchar, '''', '''''') || '''::' || v_rs_coltip.ti_column || ',';
										end if;
									end loop;

									if v_qr_actcam <> '' then
										v_qr_actcam = substring(v_qr_actcam, 1, length(v_qr_actcam) - 1);

										v_qr_return = v_qr_return || 'Update ' || v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas || chr(10) ||
											' Set ' || coalesce(v_qr_actcam, 'v_qr_actcam salio null') || chr(10);

										for v_rs_campos in
											select no_column, ti_column, nu_ordcol
											from becombas.tbtabori
											where id_compar = p_id_compar
											and no_esquem = v_rs_catori.no_esquem
											and no_tablas = v_rs_catori.no_tablas
											and va_prikey = 'S'
											order by nu_ordcol
										loop
											v_va_prikey = v_js_rowori ->> v_rs_campos.no_column;

											if v_rs_campos.nu_ordcol = 1 then
												v_qr_return = v_qr_return || 'where ' || (case when v_rs_campos.no_column is not null then v_rs_campos.no_column || ' = ''' || v_va_prikey || '''::' || v_rs_campos.ti_column else ' false' end);
											else
												v_qr_return = v_qr_return || (case when v_rs_campos.no_column is not null then ' and ' || v_rs_campos.no_column || ' = ''' || v_va_prikey || '''::' || v_rs_campos.ti_column else '' end);
											end if;
										end loop;

										v_qr_return = v_qr_return || ';' || chr(10) || chr(10);
									end if;
								end if;
							end if;
						end loop;
						
						if v_qr_insert_val <> '' then
							v_qr_return = v_qr_return || v_qr_insert || ';';
						end if;
					end if;
				end if;
			end if;

			if v_qr_return <> '' then
				v_rs_return.va_datori = v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas;
				v_rs_return.va_datdes = v_rs_catdes.no_esquem || '.' || v_rs_catdes.no_tablas;
				v_rs_return.va_sintax = v_qr_return;
				
				return next v_rs_return;
			end if;
		end loop;
	when 6 then
		-- REVISION DE FUNCIONES
		for v_rs_funori in
			select *
			from becombas.tbfunori
			where id_compar = p_id_compar
			and no_esquem not in ('fwlogneg', 'becombas')
			order by no_esquem, no_funcio, ca_parfun
		loop
			v_qr_return = null;

			select * into v_rs_fundes
			from becombas.tbfundes
			where id_compar = p_id_compar
			and no_esquem = v_rs_funori.no_esquem
			and no_funcio = v_rs_funori.no_funcio
			and ca_parfun = v_rs_funori.ca_parfun
			and ls_parfun = v_rs_funori.ls_parfun
			and ls_partyp = v_rs_funori.ls_partyp;

			if v_rs_fundes.no_funcio is null then
				v_qr_return = v_rs_funori.df_funcio;
			elseif (v_rs_funori.ls_parfun <> v_rs_fundes.ls_parfun or 
				v_rs_funori.ls_partyp <> v_rs_fundes.ls_partyp or 
				replace(replace(v_rs_funori.df_funcio, '	', ''), ' ', '') <> replace(replace(v_rs_fundes.df_funcio, '	', ''), ' ', '')
			) then
				v_qr_return = v_rs_funori.df_funcio;
			end if;

			if v_qr_return is not null then
				v_rs_return.va_datori = v_rs_funori.no_esquem || '.' || v_rs_funori.no_funcio ||'('|| rtrim(ltrim(trim(v_rs_funori.ls_parfun), '{'), '}') ||')';
				v_rs_return.va_datdes = v_rs_funori.no_esquem || '.' || v_rs_fundes.no_funcio ||'('|| rtrim(ltrim(trim(v_rs_fundes.ls_parfun), '{'), '}') ||')';
				v_rs_return.va_sintax = v_qr_return || ';';

				return next v_rs_return;
			end if;
		end loop;
	end case;
	
	return;
end;
$$;


ALTER FUNCTION becombas.f_comparar_db_sintaxis_db_central_v1(p_id_compar integer, p_ti_compar integer) OWNER TO aguzman;

--
-- Name: f_comparar_db_sintaxis_db_v4(integer, integer); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_comparar_db_sintaxis_db_v4(p_id_compar integer, p_ti_compar integer) RETURNS SETOF becombas.pdcompar2
    LANGUAGE plpgsql
    AS $$
declare
	v_rs_tabori record;
	v_rs_tabdes record;
	v_rs_funori record;
	v_rs_fundes record;
	v_rs_catori record;
	v_rs_catdes record;
	v_rs_prikey record;
	v_rs_coltip record;
	v_rs_campos record;
	v_rs_return becombas.pdcompar2%rowtype;
	v_js_conori json;
	v_js_condes json;
	v_js_rowori json;
	v_js_rowdes json;
	v_no_esquem varchar;
	v_no_tablas varchar;
	v_qr_return varchar;
	v_va_prike1 varchar;
	v_va_prike2 varchar;
	v_va_prike3 varchar;
	v_va_prike4 varchar;
	v_qr_insert varchar;
	v_qr_actcam varchar;
	v_va_prikey varchar;
	v_qr_prikey varchar;
	v_il_insert_cam boolean;
	v_qr_insert_cam varchar;
	v_qr_insert_val varchar;
begin
	/*
		select * from becombas.f_comparar_db_sintaxis_db_central_v1(45, 1);
	*/
	-- REVISION DE ESTRUCTURAS
	case p_ti_compar
	when 1 then
		for v_no_esquem in
			select distinct no_esquem
			from becombas.tbtabori
			where id_compar = p_id_compar
			and no_esquem not in ('becombas')
			order by no_esquem
		loop
			if exists (
				select distinct no_esquem from becombas.tbtabdes
				where id_compar = p_id_compar
				and no_esquem = v_no_esquem
			) then
				for v_no_tablas in
					select distinct no_tablas
					from becombas.tbtabori
					where id_compar = p_id_compar
					and no_esquem = v_no_esquem
					order by no_tablas
				loop
					if exists (
						select distinct no_tablas
						from becombas.tbtabdes
						where id_compar = p_id_compar
						and no_esquem = v_no_esquem
						and no_tablas = v_no_tablas
					) then
						for v_rs_tabori in
							select * from becombas.tbtabori
							where id_compar = p_id_compar
							and no_esquem = v_no_esquem
							and no_tablas = v_no_tablas
							order by nu_ordcol
						loop
							v_qr_return = null;

							select * into v_rs_tabdes
							from becombas.tbtabdes
							where id_compar = p_id_compar
							and no_esquem = v_no_esquem
							and no_tablas = v_no_tablas
							and no_column = v_rs_tabori.no_column;

							if v_rs_tabdes.no_column is null then -- ADD COLUMN
								v_qr_return = 'ALTER TABLE IF EXISTS '|| v_no_esquem || '.' || v_no_tablas ||
									' ADD COLUMN '|| v_rs_tabori.no_column ||
									' ' || v_rs_tabori.ti_column ||
									' ' || v_rs_tabori.va_defaul ||
									(case when v_rs_tabori.va_notnul = 'N' then ' NOT NULL' else '' end) ||
									';'
								;
							else
								if v_rs_tabori.ti_column <> v_rs_tabdes.ti_column then -- MODIFY TYPE
									v_qr_return = 'ALTER TABLE IF EXISTS '|| v_no_esquem || '.' || v_no_tablas ||
										' ALTER COLUMN '|| v_rs_tabori.no_column ||
										' TYPE ' || v_rs_tabori.ti_column ||
										';'
									;
								end if;

								if v_rs_tabori.va_defaul <> v_rs_tabdes.va_defaul then -- MODIFY DEFAULT
									v_qr_return = coalesce(v_qr_return || ' ', '') ||
										'ALTER TABLE IF EXISTS '|| v_no_esquem || '.' || v_no_tablas ||
										' ALTER COLUMN '|| v_rs_tabori.no_column ||
										(case when v_rs_tabori.va_defaul <> '' then ' SET ' || v_rs_tabori.va_defaul else ' DROP DEFAULT' end) || 
										';';
								end if;

								if v_rs_tabori.va_notnul <> v_rs_tabdes.va_notnul then -- MODIFY NOT NULL
									v_qr_return = coalesce(v_qr_return || ' ', '') ||
										'ALTER TABLE IF EXISTS '|| v_no_esquem || '.' || v_no_tablas ||
										' ALTER COLUMN '|| v_rs_tabori.no_column ||
										(case when v_rs_tabori.va_notnul = 'N' then ' SET NOT NULL' else ' DROP NOT NULL' end) ||
										';'
									;
								end if;
							end if;

							if v_qr_return is not null then
								v_rs_return.va_datori = v_rs_tabori.no_esquem ||'.'|| v_rs_tabori.no_tablas ||'.'|| v_rs_tabori.no_column;
								v_rs_return.va_datdes = v_rs_tabdes.no_esquem ||'.'|| v_rs_tabdes.no_tablas ||'.'|| v_rs_tabdes.no_column;
								v_rs_return.va_sintax = v_qr_return;

								return next v_rs_return;
							end if;
							
							if v_rs_tabori.va_forkey <> v_rs_tabdes.va_forkey then -- MODIFY FOREY_KEY
								v_rs_return.va_datori = v_rs_tabori.no_esquem ||'.'|| v_rs_tabori.no_tablas ||'.'|| v_rs_tabori.no_column;
								v_rs_return.va_datdes = v_rs_tabdes.no_esquem ||'.'|| v_rs_tabdes.no_tablas ||'.'|| v_rs_tabdes.no_column;
								v_rs_return.va_sintax = 'Revisar foreing key para este campo';

								return next v_rs_return;
							end if;
							
						end loop;	
					else	 -- CREATE TABLE
						v_qr_return := 'CREATE TABLE IF NOT EXISTS '|| v_no_esquem ||'.'|| v_no_tablas ||' (';
						
						v_qr_prikey := null;
						
						for v_rs_tabori in
							select * from becombas.tbtabori
							where id_compar = p_id_compar
							
							and no_esquem = v_no_esquem
							and no_tablas = v_no_tablas
							order by nu_ordcol
						loop
							v_qr_return := v_qr_return || chr(10) ||
								'	' || v_rs_tabori.no_column ||
								' ' || v_rs_tabori.ti_column ||
								(case when nullif(v_rs_tabori.va_defaul, '') is not null then ' ' || v_rs_tabori.va_defaul else '' end) ||
								(case when v_rs_tabori.va_notnul = 'N' then ' NOT NULL' else '' end);
								
								v_qr_return := v_qr_return || ',';
								
								if v_rs_tabori.va_prikey = 'S' then
									v_qr_prikey := coalesce(v_qr_prikey || ', ', '') || v_rs_tabori.no_column;
								end if;
						end loop;
						
						if v_qr_prikey is not null then
							v_qr_prikey := 'CONSTRAINT '|| v_no_tablas ||'_pkey PRIMARY KEY (' || v_qr_prikey || ')';
						end if;
						
						if v_qr_return is not null then
							v_rs_return.va_datori = v_no_esquem ||'.'|| v_no_tablas;
							v_rs_return.va_datdes = null;
							v_rs_return.va_sintax = v_qr_return || chr(10) || coalesce(v_qr_prikey || chr(10), '') || ');';

							return next v_rs_return;
						end if;
					end if;
				end loop;
			else	-- CREATE ESCHEMA
				v_qr_return := 'CREATE SCHEMA IF NOT EXISTS'|| v_no_esquem;
				
				if v_qr_return is not null then
					v_rs_return.va_datori = v_no_esquem;
					v_rs_return.va_datdes = v_no_esquem;
					v_rs_return.va_sintax = v_qr_return;

					return next v_rs_return;
				end if;
			end if;
		end loop;
	when 4 then
	-- REVISION DE CONTENIDO DE CATALOGOS - INSERT
		for v_rs_catori in 
			select *
			from becombas.tbcatori
			where id_compar = p_id_compar
			and no_esquem|| '.'||no_tablas not in (
				'fwconacc.trusuper',
				'fwconacc.trusumod',
				--'fwconacc.tcperfil',
				'fwconacc.trpermod',
				'fwconacc.tbmensis',
				'fwconacc.tbmodulo',
				--'cpgendoc.tcgendoc',
				--'cpgendoc.tcparame',
				--'cpgendoc.tctablas',
				--'cpgendoc.tcvariab',
            'cpperson.trperdep',
				'fwconacc.trmodblo',
				'cpexpedi.trcondat',
				'fwrecurs.trarcrec',
				'cppublic.trperage',
				'cppublic.tctartas',
				--'cppublic.tcraiexc',
				'fwgescon.tbbloque',
				'fwgescon.tbblopar',
				'fwgescon.tbblotab',
				--'fwgescon.tcregist',
				'fwgescon.tbventan',
				'fwgescon.tbvenreg',
				'fwgescon.tbventit',
				'fwgescon.tbvenbot',
				'fwgescon.tbbotacc'
			)
			--and no_esquem not in (
			--	'cpparcre',
			--	'cpmotdec'
			-- )
			order by no_esquem, no_tablas
		loop
			v_qr_return = '';
			v_qr_insert = '';
			v_qr_insert_cam = '';
			v_il_insert_cam = true;

			select * into v_rs_catdes
			from becombas.tbcatdes
			where id_compar = p_id_compar
			and no_esquem = v_rs_catori.no_esquem
			and no_tablas = v_rs_catori.no_tablas;

			v_js_conori = v_rs_catori.js_conten::json ->> 'ls_concat';
			v_js_condes = v_rs_catdes.js_conten::json ->> 'ls_concat';

			if v_rs_catdes.no_tablas is not null then
				if v_js_conori::varchar <> v_js_condes::varchar then
				-- TABLA CATALOGO ENCONTRADO Y CON DIFERENCIAS EN SU CONTENIDO
					-- IDENTIFICAMOS LA LLAVE PRIMARIA DE LA TABLA
					select
						tx[1] as co_prike1,
						tx[2] as co_prike2,
						tx[3] as co_prike3,
						tx[4] as co_prike4
					into v_rs_prikey
					from (
						select regexp_split_to_array(tx.no_column, ',')
						from (
							SELECT string_agg(no_column, ',') as no_column
							FROM becombas.tbtabori
							where id_compar = p_id_compar
							and no_esquem = v_rs_catori.no_esquem
							and no_tablas = v_rs_catori.no_tablas
							and va_prikey = 'S'
							group by id_compar, no_esquem, no_tablas
						) tx
					) as dt(tx);

					-- ARMAMOS LA SINTAXIS.
					if v_js_conori is not null then
						for v_js_rowori in
							select *
							from json_array_elements(v_js_conori::json)
						loop
							v_va_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1;
							v_va_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2;
							v_va_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3;
							v_va_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4;

							select tx.js_rowdes into v_js_rowdes
							from (
								select value as js_rowdes
								from json_array_elements(v_js_condes::json)
							) tx
							where (case when v_rs_prikey.co_prike1 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1 else true end)
							and (case when v_rs_prikey.co_prike2 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2 else true end)
							and (case when v_rs_prikey.co_prike3 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3 else true end)
							and (case when v_rs_prikey.co_prike4 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4 else true end)
							;

							if v_js_rowdes is null then
							-- Registro no encontrado, por lo tanto generamos el insert
								v_qr_insert_val = '';

								for v_rs_coltip in
									select no_column, ti_column
									from becombas.tbtabori
									where id_compar = p_id_compar
									and no_esquem = v_rs_catori.no_esquem
									and no_tablas = v_rs_catori.no_tablas
									order by nu_ordcol
								loop
									if v_il_insert_cam then
										v_qr_insert_cam = v_qr_insert_cam || v_rs_coltip.no_column || ',';
									end if;

									v_qr_insert_val = v_qr_insert_val ||  coalesce('''' || replace((v_js_rowori ->> v_rs_coltip.no_column)::varchar, '''', '''''') || '''', 'null') || '::' || v_rs_coltip.ti_column || ',';
								end loop;

								if  v_qr_insert_val <> '' then
									v_qr_insert_val = substring(v_qr_insert_val, 1, length(v_qr_insert_val) - 1);

									if v_il_insert_cam then
										v_qr_insert_cam = substring(v_qr_insert_cam, 1, length(v_qr_insert_cam) - 1);
										v_qr_insert = v_qr_insert || 'Insert into ' || v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas ||' (' || v_qr_insert_cam || ')' || chr(10);
										v_qr_insert = v_qr_insert || '	  Select ' || v_qr_insert_val || chr(10);

										v_il_insert_cam = false;
									else
										v_qr_insert = v_qr_insert || 'Union Select ' || v_qr_insert_val || chr(10);
									end if;
								end if;
							end if;
						end loop;

						if v_qr_insert_val <> '' then
							v_qr_return = v_qr_return || v_qr_insert || ';';
						end if;
					end if;
				end if;
			else
			-- NUEVA TABLA CATALOGO
				for v_js_rowori in
					select *
					from json_array_elements(v_js_conori::json)
				loop
					v_qr_insert_val = '';
					
					for v_rs_coltip in
						select no_column, ti_column
						from becombas.tbtabori
						where id_compar = p_id_compar
						and no_esquem = v_rs_catori.no_esquem
						and no_tablas = v_rs_catori.no_tablas
						order by nu_ordcol
					loop
						if v_il_insert_cam then
							v_qr_insert_cam = v_qr_insert_cam || v_rs_coltip.no_column || ', ';
						end if;
												
						v_qr_insert_val = v_qr_insert_val || 
							coalesce('''' || (v_js_rowori ->> v_rs_coltip.no_column)::varchar || '''', 'null') || '::' || v_rs_coltip.ti_column || ', '
						;
					end loop;
					
					if  v_qr_insert_val <> '' then
						v_qr_insert_val = substring(v_qr_insert_val, 1, length(v_qr_insert_val) - 1);

						if v_il_insert_cam then
							v_qr_insert_cam = substring(v_qr_insert_cam, 1, length(v_qr_insert_cam) - 1);

							v_qr_return = v_qr_return || 'Insert ' || v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas ||' (' || v_qr_insert_cam || ')' || chr(10);
							v_qr_return = v_qr_return || '	  Select ' || v_qr_insert_val || chr(10);
						else
							v_qr_return = v_qr_return || 'Union Select ' || v_qr_insert_val || chr(10);
						end if;
					end if;
					
					v_il_insert_cam = false;
				end loop;
				
				v_qr_return = v_qr_return || ';' ||chr(10);
			end if;

			if v_qr_return <> '' then
				v_rs_return.va_datori = v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas;
				v_rs_return.va_datdes = v_rs_catdes.no_esquem || '.' || v_rs_catdes.no_tablas;
				v_rs_return.va_sintax = v_qr_return;

				return next v_rs_return;
			end if;
		end loop;
	when 5 then
	-- REVISION DE CONTENIDO DE CATALOGOS - UPDATE WF
		for v_rs_catori in 
			select *
			from becombas.tbcatori
			where id_compar = p_id_compar
			and no_esquem || '.' || no_tablas not in (
				'fwconacc.trusuper',
				'fwconacc.trusumod',
				--'fwconacc.tcperfil',
				'fwconacc.trpermod',
				'fwconacc.tbmensis',
				'fwconacc.tbmodulo',
				--'cpgendoc.tcgendoc',
				--'cpgendoc.tcparame',
				--'cpgendoc.tctablas',
				--'cpgendoc.tcvariab',
				'cppublic.trperage',
				'cppublic.tctartas',
				'fwconacc.trmodblo',
				'cpexpedi.trcondat',
            'cpperson.trperdep',
				--'cpexpedi.trentdatvar',
				--'cpexpedi.trentpro',
				--'cpexpedi.trproarc',
				'fwrecurs.trarcrec',
				'fwgescon.tbbloque',
				'fwgescon.tbblopar',
				'fwgescon.tbblotab',
				'fwgescon.tbventan',
				'fwgescon.tbvenreg',
				'fwgescon.tbventit',
				'fwgescon.tbvenbot',
				'fwgescon.tbbotacc'
			)
			and no_esquem not in (
				'cpmotdec',
				'cpparcre'
			)
			order by no_esquem, no_tablas
		loop
			v_qr_return = '';
			v_qr_insert = '';
			v_qr_insert_cam = '';
			v_il_insert_cam = true;

			select * into v_rs_catdes
			from becombas.tbcatdes
			where id_compar = p_id_compar
			and no_esquem = v_rs_catori.no_esquem
			and no_tablas = v_rs_catori.no_tablas;
			
			v_js_conori = v_rs_catori.js_conten::json ->> 'ls_concat';
			v_js_condes = v_rs_catdes.js_conten::json ->> 'ls_concat';

			if v_rs_catdes.no_tablas is not null then
				if v_js_conori::varchar <> v_js_condes::varchar then
				-- TABLA CATALOGO ENCONTRADO Y CON DIFERENCIAS EN SU CONTENIDO
					-- IDENTIFICAMOS LA LLAVE PRIMARIA DE LA TABLA
					select
						tx[1] as co_prike1,
						tx[2] as co_prike2,
						tx[3] as co_prike3,
						tx[4] as co_prike4
					into v_rs_prikey
					from (
						select regexp_split_to_array(tx.no_column, ',')
						from (
							SELECT string_agg(no_column, ',') as no_column
							FROM becombas.tbtabori
							where id_compar = p_id_compar
							and no_esquem = v_rs_catori.no_esquem
							and no_tablas = v_rs_catori.no_tablas
							and va_prikey = 'S'
							group by id_compar, no_esquem, no_tablas
						) tx
					) as dt(tx);

					-- ARMAMOS LA SINTAXIS.
					if v_js_conori is not null then
						for v_js_rowori in
							select *
							from json_array_elements(v_js_conori::json)
						loop
							v_va_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1;
							v_va_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2;
							v_va_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3;
							v_va_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4;

							select tx.js_rowdes into v_js_rowdes
							from (
								select value as js_rowdes
								from json_array_elements(v_js_condes::json)
							) tx
							where (case when v_rs_prikey.co_prike1 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1 else true end)
							and (case when v_rs_prikey.co_prike2 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2 else true end)
							and (case when v_rs_prikey.co_prike3 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3 else true end)
							and (case when v_rs_prikey.co_prike4 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4 else true end)
							;

							if v_js_rowdes is not null then
							-- registro encontrado
								if v_js_rowori::varchar <> v_js_rowdes::varchar then
								-- registros diferentes
									-- generamos el update
									v_qr_actcam = '';

									for v_rs_coltip in
										select no_column, ti_column
										from becombas.tbtabori
										where id_compar = p_id_compar
										and no_esquem = v_rs_catori.no_esquem
										and no_tablas = v_rs_catori.no_tablas
										and va_prikey <> 'S'
										and no_column not ilike 'fe_%'
										and no_column <> 'us_regist'
										and no_column <> 'co_archiv'
										order by nu_ordcol
									loop
										if ((v_js_rowori ->> v_rs_coltip.no_column)::varchar <> (v_js_rowdes ->> v_rs_coltip.no_column)::varchar) or (v_js_rowdes ->> v_rs_coltip.no_column)::varchar is null then
											v_qr_actcam = v_qr_actcam || 
												v_rs_coltip.no_column || ' = ''' || replace((v_js_rowori ->> v_rs_coltip.no_column)::varchar, '''', '''''') || '''::' || v_rs_coltip.ti_column || ',';
										end if;
									end loop;

									if v_qr_actcam <> '' then
										v_qr_actcam = substring(v_qr_actcam, 1, length(v_qr_actcam) - 1);

										v_qr_return = v_qr_return || 'Update ' || v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas || chr(10) ||
											' Set ' || coalesce(v_qr_actcam, 'v_qr_actcam salio null') || chr(10);

										for v_rs_campos in
											select no_column, ti_column, nu_ordcol
											from becombas.tbtabori
											where id_compar = p_id_compar
											and no_esquem = v_rs_catori.no_esquem
											and no_tablas = v_rs_catori.no_tablas
											and va_prikey = 'S'
											order by nu_ordcol
										loop
											v_va_prikey = v_js_rowori ->> v_rs_campos.no_column;

											if v_rs_campos.nu_ordcol = 1 then
												v_qr_return = v_qr_return || 'where ' || (case when v_rs_campos.no_column is not null then v_rs_campos.no_column || ' = ''' || v_va_prikey || '''::' || v_rs_campos.ti_column else ' false' end);
											else
												v_qr_return = v_qr_return || (case when v_rs_campos.no_column is not null then ' and ' || v_rs_campos.no_column || ' = ''' || v_va_prikey || '''::' || v_rs_campos.ti_column else '' end);
											end if;
										end loop;

										v_qr_return = v_qr_return || ';' || chr(10) || chr(10);
									end if;
								end if;
							end if;
						end loop;
						
						if v_qr_insert_val <> '' then
							v_qr_return = v_qr_return || v_qr_insert || ';';
						end if;
					end if;
				end if;
			end if;

			if v_qr_return <> '' then
				v_rs_return.va_datori = v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas;
				v_rs_return.va_datdes = v_rs_catdes.no_esquem || '.' || v_rs_catdes.no_tablas;
				v_rs_return.va_sintax = v_qr_return;
				
				return next v_rs_return;
			end if;
		end loop;
	when 6 then
		-- REVISION DE FUNCIONES
		for v_rs_funori in
			select *
			from becombas.tbfunori
			where id_compar = p_id_compar
			and no_esquem not in ('becombas')
			order by no_esquem, no_funcio, ca_parfun
		loop
			v_qr_return = null;

			select * into v_rs_fundes
			from becombas.tbfundes
			where id_compar = p_id_compar
			and no_esquem = v_rs_funori.no_esquem
			and no_funcio = v_rs_funori.no_funcio
			and ca_parfun = v_rs_funori.ca_parfun
			and ls_parfun = v_rs_funori.ls_parfun
			and ls_partyp = v_rs_funori.ls_partyp;

			if v_rs_fundes.no_funcio is null then
				v_qr_return = v_rs_funori.df_funcio;
			elseif (v_rs_funori.ls_parfun <> v_rs_fundes.ls_parfun or 
				v_rs_funori.ls_partyp <> v_rs_fundes.ls_partyp or 
				replace(replace(v_rs_funori.df_funcio, '	', ''), ' ', '') <> replace(replace(v_rs_fundes.df_funcio, '	', ''), ' ', '')
			) then
				v_qr_return = v_rs_funori.df_funcio;
			end if;

			if v_qr_return is not null then
				v_rs_return.va_datori = v_rs_funori.no_esquem || '.' || v_rs_funori.no_funcio ||'('|| rtrim(ltrim(trim(v_rs_funori.ls_parfun), '{'), '}') ||')';
				v_rs_return.va_datdes = v_rs_funori.no_esquem || '.' || v_rs_fundes.no_funcio ||'('|| rtrim(ltrim(trim(v_rs_fundes.ls_parfun), '{'), '}') ||')';
				v_rs_return.va_sintax = v_qr_return || ';';

				return next v_rs_return;
			end if;
		end loop;
	end case;
	
	return;
end;
$$;


ALTER FUNCTION becombas.f_comparar_db_sintaxis_db_v4(p_id_compar integer, p_ti_compar integer) OWNER TO aguzman;

--
-- Name: f_comparar_db_sintaxis_v2(integer, integer); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_comparar_db_sintaxis_v2(p_id_compar integer, p_ti_compar integer) RETURNS SETOF becombas.pdcompar2
    LANGUAGE plpgsql
    AS $$
declare
	v_rs_tabori record;
	v_rs_tabdes record;
	v_rs_funori record;
	v_rs_fundes record;
	v_rs_catori record;
	v_rs_catdes record;
	v_rs_prikey record;
	v_rs_coltip record;
	v_rs_campos record;
	v_rs_return becombas.pdcompar2%rowtype;
	v_js_conori json;
	v_js_condes json;
	v_js_rowori json;
	v_js_rowdes json;
	v_nu_conori integer;
	v_nu_condes integer;
	v_ca_restos integer;
	v_ca_conori integer;
	v_ca_condes integer;
	v_no_esquem varchar;
	v_no_tablas varchar;
	v_qr_return varchar;
	v_va_coltip varchar;
	v_va_prike1 varchar;
	v_va_prike2 varchar;
	v_va_prike3 varchar;
	v_va_prike4 varchar;
	v_qr_insert varchar;
	v_qr_actcam varchar;
	v_va_prikey varchar;
	v_qr_prikey varchar;
	v_co_bloque varchar;
	v_co_bloant varchar := '';
	v_co_ventan varchar;
	v_co_venant varchar := '';
	v_ls_bloque varchar := '';
	v_il_insert_cam boolean;
	v_qr_insert_cam varchar;
	v_qr_insert_val varchar;
begin
	/*
		select * from becombas.f_comparar_db_sintaxis_v2(45, 1);
	*/
	-- REVISION DE ESTRUCTURAS
	case p_ti_compar
	when 3 then
	-- REVISION DE CONTENIDO DE CATALOGOS - UPDATE
		for v_rs_catori in 
			select *
			from becombas.tbcatori
			where id_compar = p_id_compar
			and no_esquem|| '.'||no_tablas in (
				'fwgescon.tbbloque',
				'fwgescon.tbblopar',
				'fwgescon.tbblotab',
				'fwgescon.tbventan',
				'fwgescon.tbvenreg',
				'fwgescon.tbventit',
				'fwgescon.tbvenbot',
				'fwgescon.tbbotacc'
			)
			order by no_esquem, no_tablas
		loop
			v_qr_return = '';
			v_qr_insert = '';
			v_qr_insert_cam = '';
			v_il_insert_cam = true;

			select * into v_rs_catdes
			from becombas.tbcatdes
			where id_compar = p_id_compar
			and no_esquem = v_rs_catori.no_esquem
			and no_tablas = v_rs_catori.no_tablas;
			
			v_js_conori = v_rs_catori.js_conten::json ->> 'ls_concat';
			v_js_condes = v_rs_catdes.js_conten::json ->> 'ls_concat';

			if v_rs_catdes.no_tablas is not null then
				if v_js_conori::varchar <> v_js_condes::varchar then
				-- TABLA CATALOGO ENCONTRADO Y CON DIFERENCIAS EN SU CONTENIDO
					-- IDENTIFICAMOS LA LLAVE PRIMARIA DE LA TABLA
					select
						tx[1] as co_prike1,
						tx[2] as co_prike2,
						tx[3] as co_prike3,
						tx[4] as co_prike4
					into v_rs_prikey
					from (
						select regexp_split_to_array(tx.no_column, ',')
						from (
							SELECT string_agg(no_column, ',') as no_column
							FROM becombas.tbtabori
							where id_compar = p_id_compar
							and no_esquem = v_rs_catori.no_esquem
							and no_tablas = v_rs_catori.no_tablas
							and va_prikey = 'S'
							group by id_compar, no_esquem, no_tablas
						) tx
					) as dt(tx);

					-- ARMAMOS LA SINTAXIS.
					if v_js_conori is not null then
						for v_js_rowori in
							select *
							from json_array_elements(v_js_conori::json)
						loop
							v_va_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1;
							v_va_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2;
							v_va_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3;
							v_va_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4;
							
							select tx.js_rowdes into v_js_rowdes
							from (
								select value as js_rowdes
								from json_array_elements(v_js_condes::json)
							) tx
							where (case when v_rs_prikey.co_prike1 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1 else true end)
							and (case when v_rs_prikey.co_prike2 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2 else true end)
							and (case when v_rs_prikey.co_prike3 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3 else true end)
							and (case when v_rs_prikey.co_prike4 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4 else true end)
							;
							
							if v_js_rowdes is not null then
							-- registro encontrado
								if v_js_rowori::varchar <> v_js_rowdes::varchar then
								-- registros diferentes
									-- generamos el update
									v_qr_actcam = '';
									
									for v_rs_coltip in
										select no_column, ti_column
										from becombas.tbtabori
										where id_compar = p_id_compar
										and no_esquem = v_rs_catori.no_esquem
										and no_tablas = v_rs_catori.no_tablas
										and va_prikey <> 'S'
										and no_column not ilike 'fe_%'
										and no_column <> 'us_regist'
										and no_column <> 'co_archiv'
										order by nu_ordcol
									loop
										if (v_js_rowori ->> v_rs_coltip.no_column)::varchar <> (v_js_rowdes ->> v_rs_coltip.no_column)::varchar then
											v_qr_actcam = v_qr_actcam || 
												v_rs_coltip.no_column || ' = ''' || (v_js_rowori ->> v_rs_coltip.no_column)::varchar || '''::' || v_rs_coltip.ti_column || ',';
										end if;
									end loop;

									if v_qr_actcam <> '' then
										v_qr_actcam = substring(v_qr_actcam, 1, length(v_qr_actcam) - 1);

										v_qr_return = v_qr_return || 'Update ' || v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas || chr(10) ||
											' Set ' || coalesce(v_qr_actcam, 'v_qr_actcam salio null') || chr(10);

										for v_rs_campos in
											select no_column, ti_column, nu_ordcol
											from becombas.tbtabori
											where id_compar = p_id_compar
											and no_esquem = v_rs_catori.no_esquem
											and no_tablas = v_rs_catori.no_tablas
											and va_prikey = 'S'
											order by nu_ordcol
										loop
											v_va_prikey = v_js_rowori ->> v_rs_campos.no_column;

											if v_rs_campos.nu_ordcol = 1 then
												v_qr_return = v_qr_return || 'where ' || (case when v_rs_campos.no_column is not null then v_rs_campos.no_column || ' = ''' || v_va_prikey || '''::' || v_rs_campos.ti_column else ' false' end);
											else
												v_qr_return = v_qr_return || (case when v_rs_campos.no_column is not null then ' and ' || v_rs_campos.no_column || ' = ''' || v_va_prikey || '''::' || v_rs_campos.ti_column else '' end);
											end if;
										end loop;

										v_qr_return = v_qr_return || ';' || chr(10) || chr(10);
									end if;
								end if;
							end if;
						end loop;
						
						if v_qr_insert_val <> '' then
							v_qr_return = v_qr_return || v_qr_insert || ';';
						end if;
					end if;
				end if;
			end if;

			if v_qr_return <> '' then
				v_rs_return.va_datori = v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas;
				v_rs_return.va_datdes = v_rs_catdes.no_esquem || '.' || v_rs_catdes.no_tablas;
				v_rs_return.va_sintax = v_qr_return;
				
				return next v_rs_return;
			end if;
		end loop;
	when 1 then
		for v_no_esquem in
			select distinct no_esquem
			from becombas.tbtabori
			where id_compar = p_id_compar
			and no_esquem not in ('cpmotdec')
			order by no_esquem
		loop
			if exists (
				select distinct no_esquem from becombas.tbtabdes
				where id_compar = p_id_compar
				and no_esquem = v_no_esquem
			) then
				for v_no_tablas in
					select distinct no_tablas
					from becombas.tbtabori
					where id_compar = p_id_compar
					and no_esquem = v_no_esquem
					order by no_tablas
				loop
					if exists (
						select distinct no_tablas
						from becombas.tbtabdes
						where id_compar = p_id_compar
						and no_esquem = v_no_esquem
						and no_tablas = v_no_tablas
					) then
						for v_rs_tabori in
							select * from becombas.tbtabori
							where id_compar = p_id_compar
							and no_esquem = v_no_esquem
							and no_tablas = v_no_tablas
							order by nu_ordcol
						loop
							v_qr_return = null;

							select * into v_rs_tabdes
							from becombas.tbtabdes
							where id_compar = p_id_compar
							and no_esquem = v_no_esquem
							and no_tablas = v_no_tablas
							and no_column = v_rs_tabori.no_column;

							if v_rs_tabdes.no_column is null then -- ADD COLUMN
								v_qr_return = 'ALTER TABLE '|| v_no_esquem || '.' || v_no_tablas ||
									' ADD COLUMN '|| v_rs_tabori.no_column ||
									' ' || v_rs_tabori.ti_column ||
									' ' || v_rs_tabori.va_defaul ||
									(case when v_rs_tabori.va_notnul = 'N' then ' NOT NULL' else '' end) ||
									';'
								;
							else
								if v_rs_tabori.ti_column <> v_rs_tabdes.ti_column then -- MODIFY TYPE
									v_qr_return = 'ALTER TABLE '|| v_no_esquem || '.' || v_no_tablas ||
										' ALTER COLUMN '|| v_rs_tabori.no_column ||
										' TYPE ' || v_rs_tabori.ti_column ||
										';'
									;
								end if;

								if v_rs_tabori.va_defaul <> v_rs_tabdes.va_defaul then -- MODIFY DEFAULT
									v_qr_return = coalesce(v_qr_return || ' ', '') ||
										'ALTER TABLE '|| v_no_esquem || '.' || v_no_tablas ||
										' ALTER COLUMN '|| v_rs_tabori.no_column ||
										(case when v_rs_tabori.va_defaul <> '' then ' SET ' || v_rs_tabori.va_defaul else ' DROP DEFAULT' end) || 
										';';
								end if;

								if v_rs_tabori.va_notnul <> v_rs_tabdes.va_notnul then -- MODIFY NOT NULL
									v_qr_return = coalesce(v_qr_return || ' ', '') ||
										'ALTER TABLE '|| v_no_esquem || '.' || v_no_tablas ||
										' ALTER COLUMN '|| v_rs_tabori.no_column ||
										(case when v_rs_tabori.va_notnul = 'N' then ' SET NOT NULL' else ' DROP NOT NULL' end) ||
										';'
									;
								end if;
							end if;

							if v_qr_return is not null then
								v_rs_return.va_datori = v_rs_tabori.no_esquem ||'.'|| v_rs_tabori.no_tablas ||'.'|| v_rs_tabori.no_column;
								v_rs_return.va_datdes = v_rs_tabdes.no_esquem ||'.'|| v_rs_tabdes.no_tablas ||'.'|| v_rs_tabdes.no_column;
								v_rs_return.va_sintax = v_qr_return;

								return next v_rs_return;
							end if;
							
							if v_rs_tabori.va_forkey <> v_rs_tabdes.va_forkey then -- MODIFY FOREY_KEY
								v_rs_return.va_datori = v_rs_tabori.no_esquem ||'.'|| v_rs_tabori.no_tablas ||'.'|| v_rs_tabori.no_column;
								v_rs_return.va_datdes = v_rs_tabdes.no_esquem ||'.'|| v_rs_tabdes.no_tablas ||'.'|| v_rs_tabdes.no_column;
								v_rs_return.va_sintax = 'Revisar forey key para este campo';

								return next v_rs_return;
							end if;
							
						end loop;	
					else	 -- CREATE TABLE
						v_qr_return := 'CREATE TABLE '|| v_no_esquem ||'.'|| v_no_tablas ||' (';
						
						v_qr_prikey := null;
						
						for v_rs_tabori in
							select * from becombas.tbtabori
							where id_compar = p_id_compar
							
							and no_esquem = v_no_esquem
							and no_tablas = v_no_tablas
							order by nu_ordcol
						loop
							v_qr_return := v_qr_return || chr(10) ||
								'	' || v_rs_tabori.no_column ||
								' ' || v_rs_tabori.ti_column ||
								(case when nullif(v_rs_tabori.va_defaul, '') is not null then v_rs_tabori.va_defaul else '' end) ||
								(case when v_rs_tabori.va_notnul = 'N' then ' NOT NULL' else '' end);
								
								v_qr_return := v_qr_return || ',';
								
								if v_rs_tabori.va_prikey = 'S' then
									v_qr_prikey := coalesce(v_qr_prikey || ', ', '') || v_rs_tabori.no_column;
								end if;
						end loop;
						
						if v_qr_prikey is not null then
							v_qr_prikey := 'CONSTRAINT '|| v_no_tablas ||'_pkey PRIMARY KEY (' || v_qr_prikey || ')';
						end if;
						
						if v_qr_return is not null then
							v_rs_return.va_datori = v_rs_tabori.no_esquem ||'.'|| v_rs_tabori.no_tablas;
							v_rs_return.va_datdes = null;
							v_rs_return.va_sintax = v_qr_return || chr(10) || coalesce(v_qr_prikey || chr(10), '') || ');';

							return next v_rs_return;
						end if;
					end if;
				end loop;
			else	-- CREATE ESCHEMA
				v_qr_return := 'CREATE SCHEMA '|| v_no_esquem;
				
				if v_qr_return is not null then
					v_rs_return.va_datori = v_rs_tabori.no_esquem;
					v_rs_return.va_datdes = v_rs_tabdes.no_esquem;
					v_rs_return.va_sintax = v_qr_return;

					return next v_rs_return;
				end if;
			end if;
		end loop;
		
	when 6 then
		-- REVISION DE FUNCIONES
		for v_rs_funori in
			select *
			from becombas.tbfunori
			where id_compar = p_id_compar
			--and no_esquem not in ('cpmotdec')
			order by no_esquem, no_funcio, ca_parfun
		loop
			v_qr_return = null;

			select * into v_rs_fundes
			from becombas.tbfundes
			where id_compar = p_id_compar
			and no_esquem = v_rs_funori.no_esquem
			and no_funcio = v_rs_funori.no_funcio
			and ca_parfun = v_rs_funori.ca_parfun
			and ls_parfun = v_rs_funori.ls_parfun
			and ls_partyp = v_rs_funori.ls_partyp;

			if v_rs_fundes.no_funcio is null then
				v_qr_return = v_rs_funori.df_funcio;
			elseif (v_rs_funori.ls_parfun <> v_rs_fundes.ls_parfun or 
				v_rs_funori.ls_partyp <> v_rs_fundes.ls_partyp or 
				replace(replace(v_rs_funori.df_funcio, '	', ''), ' ', '') <> replace(replace(v_rs_fundes.df_funcio, '	', ''), ' ', '')
			) then
				v_qr_return = v_rs_funori.df_funcio;
			end if;

			if v_qr_return is not null then
				v_rs_return.va_datori = v_rs_funori.no_esquem || '.' || v_rs_funori.no_funcio ||'('|| rtrim(ltrim(trim(v_rs_funori.ls_parfun), '{'), '}') ||')';
				v_rs_return.va_datdes = v_rs_funori.no_esquem || '.' || v_rs_fundes.no_funcio ||'('|| rtrim(ltrim(trim(v_rs_fundes.ls_parfun), '{'), '}') ||')';
				v_rs_return.va_sintax = v_qr_return || ';';

				return next v_rs_return;
			end if;
		end loop;
	when 4 then
	-- REVISION DE CONTENIDO DE CATALOGOS - INSERT
		for v_rs_catori in 
			select *
			from becombas.tbcatori
			where id_compar = p_id_compar
			and no_esquem|| '.'||no_tablas not in (
				'fwconacc.trusuper',
				'fwconacc.trusumod',
				'cpexpedi.trcondat',
				'fwrecurs.trarcrec',
				'fwgescon.tbbloque',
				'fwgescon.tbblopar',
				'fwgescon.tbblotab',
				'fwgescon.tbventan',
				'fwgescon.tbvenreg',
				'fwgescon.tbventit',
				'fwgescon.tbvenbot',
				'fwgescon.tbbotacc'
			)
			and no_esquem not in (
				'cpparcre',
				'cpmotdec'
			  )
			order by no_esquem, no_tablas
		loop
			v_qr_return = '';
			v_qr_insert = '';
			v_qr_insert_cam = '';
			v_il_insert_cam = true;

			select * into v_rs_catdes
			from becombas.tbcatdes
			where id_compar = p_id_compar
			and no_esquem = v_rs_catori.no_esquem
			and no_tablas = v_rs_catori.no_tablas;

			v_js_conori = v_rs_catori.js_conten::json ->> 'ls_concat';
			v_js_condes = v_rs_catdes.js_conten::json ->> 'ls_concat';

			if v_rs_catdes.no_tablas is not null then
				if v_js_conori::varchar <> v_js_condes::varchar then
				-- TABLA CATALOGO ENCONTRADO Y CON DIFERENCIAS EN SU CONTENIDO
					-- IDENTIFICAMOS LA LLAVE PRIMARIA DE LA TABLA
					select
						tx[1] as co_prike1,
						tx[2] as co_prike2,
						tx[3] as co_prike3,
						tx[4] as co_prike4
					into v_rs_prikey
					from (
						select regexp_split_to_array(tx.no_column, ',')
						from (
							SELECT string_agg(no_column, ',') as no_column
							FROM becombas.tbtabori
							where id_compar = p_id_compar
							and no_esquem = v_rs_catori.no_esquem
							and no_tablas = v_rs_catori.no_tablas
							and va_prikey = 'S'
							group by id_compar, no_esquem, no_tablas
						) tx
					) as dt(tx);

					-- ARMAMOS LA SINTAXIS.
					if v_js_conori is not null then
						for v_js_rowori in
							select *
							from json_array_elements(v_js_conori::json)
						loop
							v_va_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1;
							v_va_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2;
							v_va_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3;
							v_va_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4;

							select tx.js_rowdes into v_js_rowdes
							from (
								select value as js_rowdes
								from json_array_elements(v_js_condes::json)
							) tx
							where (case when v_rs_prikey.co_prike1 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1 else true end)
							and (case when v_rs_prikey.co_prike2 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2 else true end)
							and (case when v_rs_prikey.co_prike3 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3 else true end)
							and (case when v_rs_prikey.co_prike4 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4 else true end)
							;

							if v_js_rowdes is null then
							-- Registro no encontrado, por lo tanto generamos el insert
								v_qr_insert_val = '';

								for v_rs_coltip in
									select no_column, ti_column
									from becombas.tbtabori
									where id_compar = p_id_compar
									and no_esquem = v_rs_catori.no_esquem
									and no_tablas = v_rs_catori.no_tablas
									order by nu_ordcol
								loop
									if v_il_insert_cam then
										v_qr_insert_cam = v_qr_insert_cam || v_rs_coltip.no_column || ',';
									end if;

									v_qr_insert_val = v_qr_insert_val ||  coalesce('''' || (v_js_rowori ->> v_rs_coltip.no_column)::varchar || '''', 'null') || '::' || v_rs_coltip.ti_column || ',';
								end loop;

								if  v_qr_insert_val <> '' then
									v_qr_insert_val = substring(v_qr_insert_val, 1, length(v_qr_insert_val) - 1);

									if v_il_insert_cam then
										v_qr_insert_cam = substring(v_qr_insert_cam, 1, length(v_qr_insert_cam) - 1);
										v_qr_insert = v_qr_insert || 'Insert into ' || v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas ||' (' || v_qr_insert_cam || ')' || chr(10);
										v_qr_insert = v_qr_insert || '	  Select ' || v_qr_insert_val || chr(10);

										v_il_insert_cam = false;
									else
										v_qr_insert = v_qr_insert || 'Union Select ' || v_qr_insert_val || chr(10);
									end if;
								end if;
							end if;
						end loop;

						if v_qr_insert_val <> '' then
							v_qr_return = v_qr_return || v_qr_insert || ';';
						end if;
					end if;
				end if;
			else
			-- NUEVA TABLA CATALOGO
				for v_js_rowori in
					select *
					from json_array_elements(v_js_conori::json)
				loop
					v_qr_insert_val = null;

					for v_rs_coltip in
						select no_column, ti_column
						from becombas.tbtabori
						where id_compar = p_id_compar
						and no_esquem = v_rs_catori.no_esquem
						and no_tablas = v_rs_catori.no_tablas
						order by nu_ordcol
					loop
						if v_il_insert_cam then
							v_qr_insert_cam = v_qr_insert_cam || v_rs_coltip.no_column || ', ';
						end if;

						v_qr_insert_val = v_qr_insert_val || coalesce('''' || (v_js_rowori ->> v_rs_coltip.no_column)::varchar || '''', 'null') || '::' || v_rs_coltip.ti_column || ', ';
					end loop;

					if  v_qr_insert_val <> '' then
						v_qr_insert_val = substring(v_qr_insert_val, 1, length(v_qr_insert_val) - 1);

						if v_il_insert_cam then
							v_qr_insert_cam = substring(v_qr_insert_cam, 1, length(v_qr_insert_cam) - 1);

							v_qr_return = v_qr_return || 'Insert ' || v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas ||' (' || v_qr_insert_cam || ')' || chr(10);
							v_qr_return = v_qr_return || '	  Select ' || v_qr_insert_val || chr(10);

							v_il_insert_cam = false;
						else
							v_qr_return = v_qr_return || 'Union Select ' || v_qr_insert_val || chr(10);
						end if;
					end if;
				end loop;

				v_qr_return = v_qr_return || ';' ||chr(10);
			end if;

			if v_qr_return <> '' then
				v_rs_return.va_datori = v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas;
				v_rs_return.va_datdes = v_rs_catdes.no_esquem || '.' || v_rs_catdes.no_tablas;
				v_rs_return.va_sintax = v_qr_return;

				return next v_rs_return;
			end if;
		end loop;
	when 5 then
	-- REVISION DE CONTENIDO DE CATALOGOS - UPDATE
		for v_rs_catori in 
			select *
			from becombas.tbcatori
			where id_compar = p_id_compar
			and no_esquem|| '.'||no_tablas not in (
				'fwconacc.trusuper',
				'fwconacc.trusumod',
				'cpexpedi.trcondat',
				'fwrecurs.trarcrec'
			)
			and no_esquem not in (
				'cpmotdec',
				'cpparcre'
			)
			order by no_esquem, no_tablas
		loop
			v_qr_return = '';
			v_qr_insert = '';
			v_qr_insert_cam = '';
			v_il_insert_cam = true;

			select * into v_rs_catdes
			from becombas.tbcatdes
			where id_compar = p_id_compar
			and no_esquem = v_rs_catori.no_esquem
			and no_tablas = v_rs_catori.no_tablas;

			v_js_conori = v_rs_catori.js_conten::json ->> 'ls_concat';
			v_js_condes = v_rs_catdes.js_conten::json ->> 'ls_concat';

			if v_rs_catdes.no_tablas is not null then
				if v_js_conori::varchar <> v_js_condes::varchar then
				-- TABLA CATALOGO ENCONTRADO Y CON DIFERENCIAS EN SU CONTENIDO
					-- IDENTIFICAMOS LA LLAVE PRIMARIA DE LA TABLA
					select
						tx[1] as co_prike1,
						tx[2] as co_prike2,
						tx[3] as co_prike3,
						tx[4] as co_prike4
					into v_rs_prikey
					from (
						select regexp_split_to_array(tx.no_column, ',')
						from (
							SELECT string_agg(no_column, ',') as no_column
							FROM becombas.tbtabori
							where id_compar = p_id_compar
							and no_esquem = v_rs_catori.no_esquem
							and no_tablas = v_rs_catori.no_tablas
							and va_prikey = 'S'
							group by id_compar, no_esquem, no_tablas
						) tx
					) as dt(tx);

					-- ARMAMOS LA SINTAXIS.
					if v_js_conori is not null then
						for v_js_rowori in
							select *
							from json_array_elements(v_js_conori::json)
						loop
							v_va_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1;
							v_va_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2;
							v_va_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3;
							v_va_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4;

							select tx.js_rowdes into v_js_rowdes
							from (
								select value as js_rowdes
								from json_array_elements(v_js_condes::json)
							) tx
							where (case when v_rs_prikey.co_prike1 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1 else true end)
							and (case when v_rs_prikey.co_prike2 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2 else true end)
							and (case when v_rs_prikey.co_prike3 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3 else true end)
							and (case when v_rs_prikey.co_prike4 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4 else true end)
							;

							if v_js_rowdes is not null then
							-- registro encontrado
								if v_js_rowori::varchar <> v_js_rowdes::varchar
									and v_rs_catori.no_esquem ||'.'||v_rs_catori.no_tablas not in (
										'cpexpedi.trentdatvar',
										'cpexpedi.trentpro',
										'cpexpedi.trproarc',
										'cpgendoc.tcgendoc',
										'cpmotdec.tcregpolvar',
										'cpmotdec.trregpol',
										'cpparcre.thbonfam',
										'cpparcre.thcarveh'
									)
								then
								-- registros diferentes
									-- generamos el update
									v_qr_actcam = '';

									for v_rs_coltip in
										select no_column, ti_column
										from becombas.tbtabori
										where id_compar = p_id_compar
										and no_esquem = v_rs_catori.no_esquem
										and no_tablas = v_rs_catori.no_tablas
										and va_prikey <> 'S'
										and no_column not ilike 'fe_%'
										and no_column <> 'us_regist'
										and no_column <> 'co_archiv'
										order by nu_ordcol
									loop
										if ((v_js_rowori ->> v_rs_coltip.no_column)::varchar <> (v_js_rowdes ->> v_rs_coltip.no_column)::varchar) or (v_js_rowdes ->> v_rs_coltip.no_column)::varchar is null then
											v_qr_actcam = v_qr_actcam || 
												v_rs_coltip.no_column || ' = ''' || (v_js_rowori ->> v_rs_coltip.no_column)::varchar || '''::' || v_rs_coltip.ti_column || ',';
										end if;
									end loop;

									if v_qr_actcam <> '' then
										v_qr_actcam = substring(v_qr_actcam, 1, length(v_qr_actcam) - 1);

										v_qr_return = v_qr_return || 'Update ' || v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas || chr(10) ||
											' Set ' || coalesce(v_qr_actcam, 'v_qr_actcam salio null') || chr(10);

										for v_rs_campos in
											select no_column, ti_column, nu_ordcol
											from becombas.tbtabori
											where id_compar = p_id_compar
											and no_esquem = v_rs_catori.no_esquem
											and no_tablas = v_rs_catori.no_tablas
											and va_prikey = 'S'
											order by nu_ordcol
										loop
											v_va_prikey = v_js_rowori ->> v_rs_campos.no_column;

											if v_rs_campos.nu_ordcol = 1 then
												v_qr_return = v_qr_return || 'where ' || (case when v_rs_campos.no_column is not null then v_rs_campos.no_column || ' = ''' || v_va_prikey || '''::' || v_rs_campos.ti_column else ' false' end);
											else
												v_qr_return = v_qr_return || (case when v_rs_campos.no_column is not null then ' and ' || v_rs_campos.no_column || ' = ''' || v_va_prikey || '''::' || v_rs_campos.ti_column else '' end);
											end if;
										end loop;

										v_qr_return = v_qr_return || ';' || chr(10) || chr(10);
									end if;
								end if;
							end if;
						end loop;
						
						if v_qr_insert_val <> '' then
							v_qr_return = v_qr_return || v_qr_insert || ';';
						end if;
					end if;
				end if;
			end if;

			if v_qr_return <> '' then
				v_rs_return.va_datori = v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas;
				v_rs_return.va_datdes = v_rs_catdes.no_esquem || '.' || v_rs_catdes.no_tablas;
				v_rs_return.va_sintax = v_qr_return;
				
				return next v_rs_return;
			end if;
		end loop;
	when 2 then
	-- REVISION DE CONTENIDO DE CATALOGOS - INSERT
		for v_rs_catori in 
			select *
			from becombas.tbcatori
			where id_compar = p_id_compar
			and no_esquem|| '.'||no_tablas in (
				'fwgescon.tbbloque',  -- bloque
				'fwgescon.tbblopar',  -- bloque
				'fwgescon.tbblotab',  -- bloque
				'fwgescon.tbventan',  -- bloque
				'fwgescon.tbvenreg',  -- bloque
				'fwgescon.tbventit',  -- ventan
				'fwgescon.tbvenbot',  -- bloque
				'fwgescon.tbbotacc'   -- ventan
			)
			order by no_esquem, no_tablas
		loop
			v_qr_return = '';
			v_qr_insert = '';
			v_qr_insert_cam = '';
			v_il_insert_cam = true;

			select * into v_rs_catdes
			from becombas.tbcatdes
			where id_compar = p_id_compar
			and no_esquem = v_rs_catori.no_esquem
			and no_tablas = v_rs_catori.no_tablas;

			v_js_conori = v_rs_catori.js_conten::json ->> 'ls_concat';
			v_js_condes = v_rs_catdes.js_conten::json ->> 'ls_concat';

			if v_rs_catdes.no_tablas is not null then
				if v_js_conori::varchar <> v_js_condes::varchar then
				-- TABLA CATALOGO ENCONTRADO Y CON DIFERENCIAS EN SU CONTENIDO
					-- IDENTIFICAMOS LA LLAVE PRIMARIA DE LA TABLA
					select
						tx[1] as co_prike1,
						tx[2] as co_prike2,
						tx[3] as co_prike3,
						tx[4] as co_prike4
					into v_rs_prikey
					from (
						select regexp_split_to_array(tx.no_column, ',')
						from (
							SELECT string_agg(no_column, ',') as no_column
							FROM becombas.tbtabori
							where id_compar = p_id_compar
							and no_esquem = v_rs_catori.no_esquem
							and no_tablas = v_rs_catori.no_tablas
							and va_prikey = 'S'
							group by id_compar, no_esquem, no_tablas
						) tx
					) as dt(tx);

					-- ARMAMOS LA SINTAXIS.
					if v_js_conori is not null then
						for v_js_rowori in
							select * from json_array_elements(v_js_conori::json)
						loop
							v_va_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1;
							v_va_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2;
							v_va_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3;
							v_va_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4;
							
							select tx.js_rowdes into v_js_rowdes
							from (
								select value as js_rowdes
								from json_array_elements(v_js_condes::json)
							) tx
							where (case when v_rs_prikey.co_prike1 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1 else true end)
							and (case when v_rs_prikey.co_prike2 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2 else true end)
							and (case when v_rs_prikey.co_prike3 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3 else true end)
							and (case when v_rs_prikey.co_prike4 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4 else true end)
							;
							
							if v_js_rowdes is null then
							-- Registro no encontrado, por lo tanto generamos el insert
								v_qr_insert_val = '';

								for v_rs_coltip in
									select no_column, ti_column
									from becombas.tbtabori
									where id_compar = p_id_compar
									and no_esquem = v_rs_catori.no_esquem
									and no_tablas = v_rs_catori.no_tablas
									order by nu_ordcol
								loop
									if v_rs_catori.no_tablas in ('tbbloque', 'tbblopar', 'tbblotab', 'tbventan', 'tbvenreg', 'tbvenbot') then
										if v_rs_coltip.no_column = 'co_bloque' then
											v_co_bloque = (v_js_rowori ->> v_rs_coltip.no_column)::varchar;
											
											if v_co_bloque <> v_co_bloant then
												v_co_bloant = v_co_bloque;
												
												v_ls_bloque = v_ls_bloque || ' union select ' || v_co_bloque;
											end if;
										end if;
									elseif v_rs_catori.no_tablas in ('tbventit', 'tbbotacc') then
										if v_rs_coltip.no_column = 'co_ventan' then
											v_co_ventan = (v_js_rowori ->> v_rs_coltip.no_column)::varchar;
											
											if v_co_ventan <> v_co_venant then
												v_co_venant = v_co_ventan;
												
												v_ls_bloque = v_ls_bloque || ' union select (select co_bloque from fwgescon.tbventan where co_ventan = ' || v_co_ventan || ')';
											end if;
										end if;
									end if;				
								end loop;
								
								if v_ls_bloque <> '' then
									v_qr_return = v_qr_return || v_ls_bloque || chr(10);
									v_ls_bloque = '';
								end if;
							end if;
						end loop;
					end if;
				end if;
			end if;

			if v_qr_return <> '' then
				v_rs_return.va_datori = v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas;
				v_rs_return.va_datdes = v_rs_catdes.no_esquem || '.' || v_rs_catdes.no_tablas;
				v_rs_return.va_sintax = v_qr_return;

				return next v_rs_return;
			end if;
		end loop;
	
	end case;
	
	return;
end;
$$;


ALTER FUNCTION becombas.f_comparar_db_sintaxis_v2(p_id_compar integer, p_ti_compar integer) OWNER TO aguzman;

--
-- Name: f_comparar_db_sintaxis_v3(integer, integer); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_comparar_db_sintaxis_v3(p_id_compar integer, p_ti_compar integer) RETURNS SETOF becombas.pdcompar2
    LANGUAGE plpgsql
    AS $$
declare
	v_rs_tabori record;
	v_rs_tabdes record;
	v_rs_funori record;
	v_rs_fundes record;
	v_rs_catori record;
	v_rs_catdes record;
	v_rs_prikey record;
	v_rs_coltip record;
	v_rs_campos record;
	v_rs_return becombas.pdcompar2%rowtype;
	v_js_conori json;
	v_js_condes json;
	v_js_rowori json;
	v_js_rowdes json;
	v_nu_conori integer;
	v_nu_condes integer;
	v_ca_restos integer;
	v_ca_conori integer;
	v_ca_condes integer;
	v_co_bloque integer;
	v_no_esquem varchar;
	v_no_tablas varchar;
	v_qr_return varchar;
	v_va_coltip varchar;
	v_va_prike1 varchar;
	v_va_prike2 varchar;
	v_va_prike3 varchar;
	v_va_prike4 varchar;
	v_qr_insert varchar;
	v_qr_actcam varchar;
	v_va_prikey varchar;
	v_qr_prikey varchar;
	v_ls_bloque varchar;
	v_js_oritab varchar;
	v_js_oripar varchar;
	v_js_oriven varchar;
	v_js_orireg varchar;
	v_js_oribot varchar;
	v_js_oritit varchar;
	v_js_oriacc varchar;
	v_js_destab varchar;
	v_js_despar varchar;
	v_js_desven varchar;
	v_js_desreg varchar;
	v_js_desbot varchar;
	v_js_destit varchar;
	v_js_desacc varchar;
	v_ls_oriven varchar;
	v_ls_desven varchar;
	v_il_insert_cam boolean;
	v_qr_insert_cam varchar;
	v_qr_insert_val varchar;
	v_il_difblo boolean;
begin
	/*
		select * from becombas.f_comparar_db_sintaxis_v3(45, 1);
	*/
	-- REVISION DE ESTRUCTURAS
	case p_ti_compar
	when 1 then
		for v_no_esquem in
			select distinct no_esquem
			from becombas.tbtabori
			where id_compar = p_id_compar
			order by no_esquem
		loop
			if exists (
				select distinct no_esquem from becombas.tbtabdes
				where id_compar = p_id_compar
				and no_esquem = v_no_esquem
			) then
				for v_no_tablas in
					select distinct no_tablas
					from becombas.tbtabori
					where id_compar = p_id_compar
					and no_esquem = v_no_esquem
					order by no_tablas
				loop
					if exists (
						select distinct no_tablas
						from becombas.tbtabdes
						where id_compar = p_id_compar
						and no_esquem = v_no_esquem
						and no_tablas = v_no_tablas
					) then
						for v_rs_tabori in
							select * from becombas.tbtabori
							where id_compar = p_id_compar
							and no_esquem = v_no_esquem
							and no_tablas = v_no_tablas
							order by nu_ordcol
						loop
							v_qr_return = null;

							select * into v_rs_tabdes
							from becombas.tbtabdes
							where id_compar = p_id_compar
							and no_esquem = v_no_esquem
							and no_tablas = v_no_tablas
							and no_column = v_rs_tabori.no_column;

							if v_rs_tabdes.no_column is null then -- ADD COLUMN
								v_qr_return = 'ALTER TABLE '|| v_no_esquem || '.' || v_no_tablas ||
									' ADD COLUMN '|| v_rs_tabori.no_column ||
									' ' || v_rs_tabori.ti_column ||
									' ' || v_rs_tabori.va_defaul ||
									(case when v_rs_tabori.va_notnul = 'N' then ' NOT NULL' else '' end) ||
									';'
								;
							else
								if v_rs_tabori.ti_column <> v_rs_tabdes.ti_column then -- MODIFY TYPE
									v_qr_return = 'ALTER TABLE '|| v_no_esquem || '.' || v_no_tablas ||
										' ALTER COLUMN '|| v_rs_tabori.no_column ||
										' TYPE ' || v_rs_tabori.ti_column ||
										';'
									;
								end if;

								if v_rs_tabori.va_defaul <> v_rs_tabdes.va_defaul then -- MODIFY DEFAULT
									v_qr_return = coalesce(v_qr_return || ' ', '') ||
										'ALTER TABLE '|| v_no_esquem || '.' || v_no_tablas ||
										' ALTER COLUMN '|| v_rs_tabori.no_column ||
										(case when v_rs_tabori.va_defaul <> '' then ' SET ' || v_rs_tabori.va_defaul else ' DROP DEFAULT' end) || 
										';';
								end if;

								if v_rs_tabori.va_notnul <> v_rs_tabdes.va_notnul then -- MODIFY NOT NULL
									v_qr_return = coalesce(v_qr_return || ' ', '') ||
										'ALTER TABLE '|| v_no_esquem || '.' || v_no_tablas ||
										' ALTER COLUMN '|| v_rs_tabori.no_column ||
										(case when v_rs_tabori.va_notnul = 'N' then ' SET NOT NULL' else ' DROP NOT NULL' end) ||
										';'
									;
								end if;
							end if;

							if v_qr_return is not null then
								v_rs_return.va_datori = v_rs_tabori.no_esquem ||'.'|| v_rs_tabori.no_tablas ||'.'|| v_rs_tabori.no_column;
								v_rs_return.va_datdes = v_rs_tabdes.no_esquem ||'.'|| v_rs_tabdes.no_tablas ||'.'|| v_rs_tabdes.no_column;
								v_rs_return.va_sintax = v_qr_return;

								return next v_rs_return;
							end if;
							
							if v_rs_tabori.va_forkey <> v_rs_tabdes.va_forkey then -- MODIFY FOREY_KEY
								v_rs_return.va_datori = v_rs_tabori.no_esquem ||'.'|| v_rs_tabori.no_tablas ||'.'|| v_rs_tabori.no_column;
								v_rs_return.va_datdes = v_rs_tabdes.no_esquem ||'.'|| v_rs_tabdes.no_tablas ||'.'|| v_rs_tabdes.no_column;
								v_rs_return.va_sintax = 'Revisar forey key para este campo';

								return next v_rs_return;
							end if;
							
						end loop;	
					else	 -- CREATE TABLE
						v_qr_return := 'CREATE TABLE '|| v_no_esquem ||'.'|| v_no_tablas ||' (';
						
						v_qr_prikey := null;
						
						for v_rs_tabori in
							select * from becombas.tbtabori
							where id_compar = p_id_compar
							
							and no_esquem = v_no_esquem
							and no_tablas = v_no_tablas
							order by nu_ordcol
						loop
							v_qr_return := v_qr_return || chr(10) ||
								'	' || v_rs_tabori.no_column ||
								' ' || v_rs_tabori.ti_column ||
								(case when nullif(v_rs_tabori.va_defaul, '') is not null then v_rs_tabori.va_defaul else '' end) ||
								(case when v_rs_tabori.va_notnul = 'N' then ' NOT NULL' else '' end);
								
								v_qr_return := v_qr_return || ',';
								
								if v_rs_tabori.va_prikey = 'S' then
									v_qr_prikey := coalesce(v_qr_prikey || ', ', '') || v_rs_tabori.no_column;
								end if;
						end loop;
						
						if v_qr_prikey is not null then
							v_qr_prikey := 'CONSTRAINT '|| v_no_tablas ||'_pkey PRIMARY KEY (' || v_qr_prikey || ')';
						end if;
						
						if v_qr_return is not null then
							v_rs_return.va_datori = v_rs_tabori.no_esquem ||'.'|| v_rs_tabori.no_tablas;
							v_rs_return.va_datdes = null;
							v_rs_return.va_sintax = v_qr_return || chr(10) || coalesce(v_qr_prikey || chr(10), '') || ');';

							return next v_rs_return;
						end if;
					end if;
				end loop;
			else	-- CREATE ESCHEMA
				v_qr_return := 'CREATE SCHEMA '|| v_no_esquem;
				
				if v_qr_return is not null then
					v_rs_return.va_datori = v_rs_tabori.no_esquem;
					v_rs_return.va_datdes = v_rs_tabdes.no_esquem;
					v_rs_return.va_sintax = v_qr_return;

					return next v_rs_return;
				end if;
			end if;
		end loop;
	when 2 then
	-- REVISION DE CONTENIDO DE CATALOGOS FRAMEWORK - INSERT FW
		select * into v_rs_catori
		from becombas.tbcatori
		where id_compar = p_id_compar
		and no_esquem = 'fwgescon'
		and no_tablas = 'tbbloque';
		
		select * into v_rs_catdes
		from becombas.tbcatdes
		where id_compar = p_id_compar
		and no_esquem = 'fwgescon'
		and no_tablas = 'tbbloque';
		
		v_js_conori := v_rs_catori.js_conten::json ->> 'ls_concat';
		v_js_condes := v_rs_catdes.js_conten::json ->> 'ls_concat';
		
		-- ARMAMOS LA SINTAXIS.
		if v_js_conori is not null then
			for v_js_rowori in
				select * from json_array_elements(v_js_conori::json)
			loop
				v_il_difblo := false;
				v_co_bloque = (v_js_rowori ->> 'co_bloque')::integer;
				
				select tx.js_rowdes into v_js_rowdes
				from (
					select value as js_rowdes
					from json_array_elements(v_js_condes::json)
				) tx
				where (tx.js_rowdes ->> 'co_bloque')::integer = v_co_bloque;
				
				if v_js_rowdes is not null then  -- bloque nuevo
					v_js_oritab := (
						select string_agg(js_rowori::varchar, ',')
						from (
							select js_rowori
							from json_array_elements((
								select js_conten::json -> 'ls_concat'
								from becombas.tbcatori
								where id_compar = p_id_compar
								and no_esquem = 'fwgescon'
								and no_tablas = 'tbblotab'
							)) js_rowori
						) tx
						where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
					);

					v_js_destab := (
						select string_agg(js_rowori::varchar, ',')
						from (
							select js_rowori
							from json_array_elements((
								select js_conten::json -> 'ls_concat'
								from becombas.tbcatdes
								where id_compar = p_id_compar
								and no_esquem = 'fwgescon'
								and no_tablas = 'tbblotab'
							)) js_rowori
						) tx
						where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
					);
					
					if (v_js_oritab = v_js_destab) or (v_js_oritab is null and v_js_destab is null) then
						v_js_oripar := (
							select string_agg(js_rowori::varchar, ',')
							from (
								select js_rowori
								from json_array_elements((
									select js_conten::json -> 'ls_concat'
									from becombas.tbcatori
									where id_compar = p_id_compar
									and no_esquem = 'fwgescon'
									and no_tablas = 'tbblopar'
								)) js_rowori
							) tx
							where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
						);
						
						v_js_despar := (
							select string_agg(js_rowori::varchar, ',')
							from (
								select js_rowori
								from json_array_elements(
									(select js_conten::json -> 'ls_concat'
									from becombas.tbcatdes
									where id_compar = p_id_compar
									and no_esquem = 'fwgescon'
									and no_tablas = 'tbblopar')
								) js_rowori
							) tx
							where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
						);
						
						if (v_js_oripar = v_js_despar) or (v_js_oripar is null and v_js_despar is null) then
							v_js_oriven := (
								select string_agg(js_rowori::varchar, ',')
								from (
									select js_rowori
									from json_array_elements(
										(select js_conten::json -> 'ls_concat'
										from becombas.tbcatori
										where id_compar = p_id_compar
										and no_esquem = 'fwgescon'
										and no_tablas = 'tbventan')
									) js_rowori
								) tx
								where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
							);
							
							v_js_desven := (
								select string_agg(js_rowori::varchar, ',')
								from (
									select js_rowori
									from json_array_elements(
										(select js_conten::json -> 'ls_concat'
										from becombas.tbcatdes
										where id_compar = p_id_compar
										and no_esquem = 'fwgescon'
										and no_tablas = 'tbventan')
									) js_rowori
								) tx
								where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
							);
							
							if (v_js_oriven = v_js_desven) or (v_js_oriven is null and v_js_desven is null) then
								v_js_oribot := (
									select string_agg(js_rowori::varchar, ',')
									from (
										select js_rowori
										from json_array_elements(
											(select js_conten::json -> 'ls_concat'
											from becombas.tbcatori
											where id_compar = p_id_compar
											and no_esquem = 'fwgescon'
											and no_tablas = 'tbvenbot')
										) js_rowori
									) tx
									where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
								);

								v_js_desbot := (
									select string_agg(js_rowori::varchar, ',')
									from (
										select js_rowori
										from json_array_elements(
											(select js_conten::json -> 'ls_concat'
											from becombas.tbcatdes
											where id_compar = p_id_compar
											and no_esquem = 'fwgescon'
											and no_tablas = 'tbvenbot')
										) js_rowori
									) tx
									where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
								);
								
								if (v_js_oribot = v_js_desbot) or (v_js_oribot is null and v_js_desbot is null) then
									v_js_orireg := (
										select string_agg(js_rowori::varchar, ',')
										from (
											select js_rowori
											from json_array_elements(
												(select js_conten::json -> 'ls_concat'
												from becombas.tbcatori
												where id_compar = p_id_compar
												and no_esquem = 'fwgescon'
												and no_tablas = 'tbvenreg')
											) js_rowori
										) tx
										where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
									);

									v_js_desreg := (
										select string_agg(js_rowori::varchar, ',')
										from (
											select js_rowori
											from json_array_elements(
												(select js_conten::json -> 'ls_concat'
												from becombas.tbcatdes
												where id_compar = p_id_compar
												and no_esquem = 'fwgescon'
												and no_tablas = 'tbvenreg')
											) js_rowori
										) tx
										where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
									);
									
									if (v_js_orireg = v_js_desreg) or (v_js_orireg is null and v_js_desreg is null) then
										v_ls_oriven := (
											select string_agg(tx.js_rowori ->> 'co_ventan', ',')
											from (
												select js_rowori
												from json_array_elements((
													select js_conten::json -> 'ls_concat'
													from becombas.tbcatori
													where id_compar = p_id_compar
													and no_esquem = 'fwgescon'
													and no_tablas = 'tbventan'
												)) js_rowori
											) tx
											where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
										);

										v_ls_desven := (
											select string_agg(tx.js_rowori ->> 'co_ventan', ',')
											from (
												select js_rowori
												from json_array_elements((
													select js_conten::json -> 'ls_concat'
													from becombas.tbcatdes
													where id_compar = p_id_compar
													and no_esquem = 'fwgescon'
													and no_tablas = 'tbventan'
												)) js_rowori
											) tx
											where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
										);

										v_js_oritit := (
											select string_agg(js_rowori::varchar, ',')
											from (
												select js_rowori
												from json_array_elements(
													(select js_conten::json -> 'ls_concat'
													from becombas.tbcatori
													where id_compar = p_id_compar
													and no_esquem = 'fwgescon'
													and no_tablas = 'tbventit')
												) js_rowori
											) tx
											where (tx.js_rowori ->> 'co_ventan')::integer in (
												select co_ventan::integer 
												from regexp_split_to_table(v_ls_oriven, ',') as co_ventan
											)
										);

										v_js_destit := (
											select string_agg(js_rowori::varchar, ',')
											from (
												select js_rowori
												from json_array_elements(
													(select js_conten::json -> 'ls_concat'
													from becombas.tbcatdes
													where id_compar = p_id_compar
													and no_esquem = 'fwgescon'
													and no_tablas = 'tbventit')
												) js_rowori
											) tx
											where (tx.js_rowori ->> 'co_ventan')::integer in (
												select co_ventan::integer 
												from regexp_split_to_table(v_ls_desven, ',') as co_ventan
											)
										);
										
										if (v_js_oritit = v_js_destit) or (v_js_oritit is null and v_js_destit is null) then
											v_js_oriacc := (
												select string_agg(js_rowori::varchar, ',')
												from (
													select js_rowori
													from json_array_elements(
														(select js_conten::json -> 'ls_concat'
														from becombas.tbcatori
														where id_compar = p_id_compar
														and no_esquem = 'fwgescon'
														and no_tablas = 'tbbotacc')
													) js_rowori
												) tx
												where (tx.js_rowori ->> 'co_ventan')::integer in (
													select co_ventan::integer 
													from regexp_split_to_table(v_ls_oriven, ',') as co_ventan
												) 
											);

											v_js_desacc := (
												select string_agg(js_rowori::varchar, ',')
												from (
													select js_rowori
													from json_array_elements(
														(select js_conten::json -> 'ls_concat'
														from becombas.tbcatdes
														where id_compar = p_id_compar
														and no_esquem = 'fwgescon'
														and no_tablas = 'tbbotacc')
													) js_rowori
												) tx
												where (tx.js_rowori ->> 'co_ventan')::integer in (
													select co_ventan::integer 
													from regexp_split_to_table(v_ls_desven, ',') as co_ventan
												) 
											);
											
											if (v_js_oriacc = v_js_desacc) or (v_js_oriacc is null and v_js_desacc is null) then
											else
												v_il_difblo := true;
											end if;
										else
											v_il_difblo := true;
										end if;
									else
										v_il_difblo := true;
									end if;
								else
									v_il_difblo := true;
								end if;
							else
								v_il_difblo := true;
							end if;
						else
							v_il_difblo := true;
						end if;
					else
						v_il_difblo := true;
					end if;
				else
					v_il_difblo := true;
				end if;
				
				if v_il_difblo then
					v_ls_bloque := (case when v_ls_bloque is null then v_co_bloque::varchar else v_ls_bloque || ',' || v_co_bloque::varchar end);
				end if;
			end loop;
		end if;
		
		v_rs_return.va_datori = v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas;
		v_rs_return.va_datdes = v_rs_catdes.no_esquem || '.' || v_rs_catdes.no_tablas;
		v_rs_return.va_sintax = v_ls_bloque;

		return next v_rs_return;
	when 3 then
	-- REVISION DE CONTENIDO DE CATALOGOS - UPDATE
		for v_rs_catori in 
			select *
			from becombas.tbcatori
			where id_compar = p_id_compar
			and no_esquem = 'fwgescon'
			and no_tablas = 'tbbloque'
			order by no_esquem, no_tablas
		loop
			v_qr_return = '';
			v_qr_insert = '';
			v_qr_insert_cam = '';
			v_il_insert_cam = true;

			select * into v_rs_catdes
			from becombas.tbcatdes
			where id_compar = p_id_compar
			and no_esquem = v_rs_catori.no_esquem
			and no_tablas = v_rs_catori.no_tablas;
			
			v_js_conori = v_rs_catori.js_conten::json ->> 'ls_concat';
			v_js_condes = v_rs_catdes.js_conten::json ->> 'ls_concat';

			if v_rs_catdes.no_tablas is not null then
				if v_js_conori::varchar <> v_js_condes::varchar then
				-- TABLA CATALOGO ENCONTRADO Y CON DIFERENCIAS EN SU CONTENIDO
					-- IDENTIFICAMOS LA LLAVE PRIMARIA DE LA TABLA
					select
						tx[1] as co_prike1,
						tx[2] as co_prike2,
						tx[3] as co_prike3,
						tx[4] as co_prike4
					into v_rs_prikey
					from (
						select regexp_split_to_array(tx.no_column, ',')
						from (
							SELECT string_agg(no_column, ',') as no_column
							FROM becombas.tbtabori
							where id_compar = p_id_compar
							and no_esquem = v_rs_catori.no_esquem
							and no_tablas = v_rs_catori.no_tablas
							and va_prikey = 'S'
							group by id_compar, no_esquem, no_tablas
						) tx
					) as dt(tx);

					-- ARMAMOS LA SINTAXIS.
					if v_js_conori is not null then
						for v_js_rowori in
							select *
							from json_array_elements(v_js_conori::json)
						loop
							v_va_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1;
							v_va_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2;
							v_va_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3;
							v_va_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4;
							
							select tx.js_rowdes into v_js_rowdes
							from (
								select value as js_rowdes
								from json_array_elements(v_js_condes::json)
							) tx
							where (case when v_rs_prikey.co_prike1 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1 else true end)
							and (case when v_rs_prikey.co_prike2 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2 else true end)
							and (case when v_rs_prikey.co_prike3 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3 else true end)
							and (case when v_rs_prikey.co_prike4 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4 else true end)
							;
							
							if v_js_rowdes is not null then
							-- registro encontrado
								if v_js_rowori::varchar <> v_js_rowdes::varchar then
								-- registros diferentes
									-- generamos el update
									v_qr_actcam = '';
									
									for v_rs_coltip in
										select no_column, ti_column
										from becombas.tbtabori
										where id_compar = p_id_compar
										and no_esquem = v_rs_catori.no_esquem
										and no_tablas = v_rs_catori.no_tablas
										and va_prikey <> 'S'
										and no_column not ilike 'fe_%'
										and no_column <> 'us_regist'
										and no_column <> 'co_archiv'
										order by nu_ordcol
									loop
										if (v_js_rowori ->> v_rs_coltip.no_column)::varchar <> (v_js_rowdes ->> v_rs_coltip.no_column)::varchar then
											v_qr_actcam = v_qr_actcam || 
												v_rs_coltip.no_column || ' = ''' || (v_js_rowori ->> v_rs_coltip.no_column)::varchar || '''::' || v_rs_coltip.ti_column || ',';
										end if;
									end loop;

									if v_qr_actcam <> '' then
										v_qr_actcam = substring(v_qr_actcam, 1, length(v_qr_actcam) - 1);

										v_qr_return = v_qr_return || 'Update ' || v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas || chr(10) ||
											' Set ' || coalesce(v_qr_actcam, 'v_qr_actcam salio null') || chr(10);

										for v_rs_campos in
											select no_column, ti_column, nu_ordcol
											from becombas.tbtabori
											where id_compar = p_id_compar
											and no_esquem = v_rs_catori.no_esquem
											and no_tablas = v_rs_catori.no_tablas
											and va_prikey = 'S'
											order by nu_ordcol
										loop
											v_va_prikey = v_js_rowori ->> v_rs_campos.no_column;

											if v_rs_campos.nu_ordcol = 1 then
												v_qr_return = v_qr_return || 'where ' || (case when v_rs_campos.no_column is not null then v_rs_campos.no_column || ' = ''' || v_va_prikey || '''::' || v_rs_campos.ti_column else ' false' end);
											else
												v_qr_return = v_qr_return || (case when v_rs_campos.no_column is not null then ' and ' || v_rs_campos.no_column || ' = ''' || v_va_prikey || '''::' || v_rs_campos.ti_column else '' end);
											end if;
										end loop;

										v_qr_return = v_qr_return || ';' || chr(10) || chr(10);
									end if;
								end if;
							end if;
						end loop;
						
						if v_qr_insert_val <> '' then
							v_qr_return = v_qr_return || v_qr_insert || ';';
						end if;
					end if;
				end if;
			end if;

			if v_qr_return <> '' then
				v_rs_return.va_datori = v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas;
				v_rs_return.va_datdes = v_rs_catdes.no_esquem || '.' || v_rs_catdes.no_tablas;
				v_rs_return.va_sintax = v_qr_return;
				
				return next v_rs_return;
			end if;
		end loop;
	when 4 then
	-- REVISION DE CONTENIDO DE CATALOGOS - INSERT
		for v_rs_catori in 
			select *
			from becombas.tbcatori
			where id_compar = p_id_compar
			and no_esquem|| '.'||no_tablas not in (
				'fwconacc.trusuper',
				'fwconacc.trusumod',
				'fwconacc.tcperfil',
				'fwconacc.trpermod',
				'fwconacc.tbmensis',
				'fwconacc.tbmodulo',
				'cpgendoc.tcgendoc',
				'cpgendoc.tcparame',
				'cpgendoc.tctablas',
				'cpgendoc.tcvariab',
				'fwconacc.trmodblo',
				'cpexpedi.trcondat',
				'fwrecurs.trarcrec',
				'cppublic.trperage',
				'fwgescon.tbbloque',
				'fwgescon.tbblopar',
				'fwgescon.tbblotab',
				'fwgescon.tbventan',
				'fwgescon.tbvenreg',
				'fwgescon.tbventit',
				'fwgescon.tbvenbot',
				'fwgescon.tbbotacc'
			)
			and no_esquem not in (
				'cpparcre',
				'cpmotdec'
			  )
			order by no_esquem, no_tablas
		loop
			v_qr_return = '';
			v_qr_insert = '';
			v_qr_insert_cam = '';
			v_il_insert_cam = true;

			select * into v_rs_catdes
			from becombas.tbcatdes
			where id_compar = p_id_compar
			and no_esquem = v_rs_catori.no_esquem
			and no_tablas = v_rs_catori.no_tablas;

			v_js_conori = v_rs_catori.js_conten::json ->> 'ls_concat';
			v_js_condes = v_rs_catdes.js_conten::json ->> 'ls_concat';

			if v_rs_catdes.no_tablas is not null then
				if v_js_conori::varchar <> v_js_condes::varchar then
				-- TABLA CATALOGO ENCONTRADO Y CON DIFERENCIAS EN SU CONTENIDO
					-- IDENTIFICAMOS LA LLAVE PRIMARIA DE LA TABLA
					select
						tx[1] as co_prike1,
						tx[2] as co_prike2,
						tx[3] as co_prike3,
						tx[4] as co_prike4
					into v_rs_prikey
					from (
						select regexp_split_to_array(tx.no_column, ',')
						from (
							SELECT string_agg(no_column, ',') as no_column
							FROM becombas.tbtabori
							where id_compar = p_id_compar
							and no_esquem = v_rs_catori.no_esquem
							and no_tablas = v_rs_catori.no_tablas
							and va_prikey = 'S'
							group by id_compar, no_esquem, no_tablas
						) tx
					) as dt(tx);

					-- ARMAMOS LA SINTAXIS.
					if v_js_conori is not null then
						for v_js_rowori in
							select *
							from json_array_elements(v_js_conori::json)
						loop
							v_va_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1;
							v_va_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2;
							v_va_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3;
							v_va_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4;

							select tx.js_rowdes into v_js_rowdes
							from (
								select value as js_rowdes
								from json_array_elements(v_js_condes::json)
							) tx
							where (case when v_rs_prikey.co_prike1 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1 else true end)
							and (case when v_rs_prikey.co_prike2 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2 else true end)
							and (case when v_rs_prikey.co_prike3 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3 else true end)
							and (case when v_rs_prikey.co_prike4 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4 else true end)
							;

							if v_js_rowdes is null then
							-- Registro no encontrado, por lo tanto generamos el insert
								v_qr_insert_val = '';

								for v_rs_coltip in
									select no_column, ti_column
									from becombas.tbtabori
									where id_compar = p_id_compar
									and no_esquem = v_rs_catori.no_esquem
									and no_tablas = v_rs_catori.no_tablas
									order by nu_ordcol
								loop
									if v_il_insert_cam then
										v_qr_insert_cam = v_qr_insert_cam || v_rs_coltip.no_column || ',';
									end if;

									v_qr_insert_val = v_qr_insert_val ||  coalesce('''' || replace((v_js_rowori ->> v_rs_coltip.no_column)::varchar, '''', '''''') || '''', 'null') || '::' || v_rs_coltip.ti_column || ',';
								end loop;

								if  v_qr_insert_val <> '' then
									v_qr_insert_val = substring(v_qr_insert_val, 1, length(v_qr_insert_val) - 1);

									if v_il_insert_cam then
										v_qr_insert_cam = substring(v_qr_insert_cam, 1, length(v_qr_insert_cam) - 1);
										v_qr_insert = v_qr_insert || 'Insert into ' || v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas ||' (' || v_qr_insert_cam || ')' || chr(10);
										v_qr_insert = v_qr_insert || '	  Select ' || v_qr_insert_val || chr(10);

										v_il_insert_cam = false;
									else
										v_qr_insert = v_qr_insert || 'Union Select ' || v_qr_insert_val || chr(10);
									end if;
								end if;
							end if;
						end loop;

						if v_qr_insert_val <> '' then
							v_qr_return = v_qr_return || v_qr_insert || ';';
						end if;
					end if;
				end if;
			else
			-- NUEVA TABLA CATALOGO
				for v_js_rowori in
					select *
					from json_array_elements(v_js_conori::json)
				loop
					v_qr_insert_val = null;

					for v_rs_coltip in
						select no_column, ti_column
						from becombas.tbtabori
						where id_compar = p_id_compar
						and no_esquem = v_rs_catori.no_esquem
						and no_tablas = v_rs_catori.no_tablas
						order by nu_ordcol
					loop
						if v_il_insert_cam then
							v_qr_insert_cam = v_qr_insert_cam || v_rs_coltip.no_column || ', ';
						end if;

						v_qr_insert_val = v_qr_insert_val || coalesce('''' || (v_js_rowori ->> v_rs_coltip.no_column)::varchar || '''', 'null') || '::' || v_rs_coltip.ti_column || ', ';
					end loop;

					if  v_qr_insert_val <> '' then
						v_qr_insert_val = substring(v_qr_insert_val, 1, length(v_qr_insert_val) - 1);

						if v_il_insert_cam then
							v_qr_insert_cam = substring(v_qr_insert_cam, 1, length(v_qr_insert_cam) - 1);

							v_qr_return = v_qr_return || 'Insert ' || v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas ||' (' || v_qr_insert_cam || ')' || chr(10);
							v_qr_return = v_qr_return || '	  Select ' || v_qr_insert_val || chr(10);

							v_il_insert_cam = false;
						else
							v_qr_return = v_qr_return || 'Union Select ' || v_qr_insert_val || chr(10);
						end if;
					end if;
				end loop;

				v_qr_return = v_qr_return || ';' ||chr(10);
			end if;

			if v_qr_return <> '' then
				v_rs_return.va_datori = v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas;
				v_rs_return.va_datdes = v_rs_catdes.no_esquem || '.' || v_rs_catdes.no_tablas;
				v_rs_return.va_sintax = v_qr_return;

				return next v_rs_return;
			end if;
		end loop;
	when 5 then
	-- REVISION DE CONTENIDO DE CATALOGOS - UPDATE WF
		for v_rs_catori in 
			select *
			from becombas.tbcatori
			where id_compar = p_id_compar
			and no_esquem || '.' || no_tablas not in (
				'fwconacc.trusuper',
				'fwconacc.trusumod',
				'fwconacc.tcperfil',
				'fwconacc.trpermod',
				'fwconacc.tbmensis',
				'fwconacc.tbmodulo',
				'cpgendoc.tcgendoc',
				'cpgendoc.tcparame',
				'cpgendoc.tctablas',
				'cpgendoc.tcvariab',
				'cppublic.trperage',
				'fwconacc.trmodblo',
				'cpexpedi.trcondat',
				'cpexpedi.trentdatvar',
				'cpexpedi.trentpro',
				'cpexpedi.trproarc',
				'fwrecurs.trarcrec',
				'fwgescon.tbbloque',
				'fwgescon.tbblopar',
				'fwgescon.tbblotab',
				'fwgescon.tbventan',
				'fwgescon.tbvenreg',
				'fwgescon.tbventit',
				'fwgescon.tbvenbot',
				'fwgescon.tbbotacc'
			)
			and no_esquem not in (
				'cpmotdec',
				'cpparcre'
			)
			order by no_esquem, no_tablas
		loop
			v_qr_return = '';
			v_qr_insert = '';
			v_qr_insert_cam = '';
			v_il_insert_cam = true;

			select * into v_rs_catdes
			from becombas.tbcatdes
			where id_compar = p_id_compar
			and no_esquem = v_rs_catori.no_esquem
			and no_tablas = v_rs_catori.no_tablas;
			
			v_js_conori = v_rs_catori.js_conten::json ->> 'ls_concat';
			v_js_condes = v_rs_catdes.js_conten::json ->> 'ls_concat';

			if v_rs_catdes.no_tablas is not null then
				if v_js_conori::varchar <> v_js_condes::varchar then
				-- TABLA CATALOGO ENCONTRADO Y CON DIFERENCIAS EN SU CONTENIDO
					-- IDENTIFICAMOS LA LLAVE PRIMARIA DE LA TABLA
					select
						tx[1] as co_prike1,
						tx[2] as co_prike2,
						tx[3] as co_prike3,
						tx[4] as co_prike4
					into v_rs_prikey
					from (
						select regexp_split_to_array(tx.no_column, ',')
						from (
							SELECT string_agg(no_column, ',') as no_column
							FROM becombas.tbtabori
							where id_compar = p_id_compar
							and no_esquem = v_rs_catori.no_esquem
							and no_tablas = v_rs_catori.no_tablas
							and va_prikey = 'S'
							group by id_compar, no_esquem, no_tablas
						) tx
					) as dt(tx);

					-- ARMAMOS LA SINTAXIS.
					if v_js_conori is not null then
						for v_js_rowori in
							select *
							from json_array_elements(v_js_conori::json)
						loop
							v_va_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1;
							v_va_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2;
							v_va_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3;
							v_va_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4;

							select tx.js_rowdes into v_js_rowdes
							from (
								select value as js_rowdes
								from json_array_elements(v_js_condes::json)
							) tx
							where (case when v_rs_prikey.co_prike1 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike1 = v_js_rowori ->> v_rs_prikey.co_prike1 else true end)
							and (case when v_rs_prikey.co_prike2 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike2 = v_js_rowori ->> v_rs_prikey.co_prike2 else true end)
							and (case when v_rs_prikey.co_prike3 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike3 = v_js_rowori ->> v_rs_prikey.co_prike3 else true end)
							and (case when v_rs_prikey.co_prike4 is not null then tx.js_rowdes ->> v_rs_prikey.co_prike4 = v_js_rowori ->> v_rs_prikey.co_prike4 else true end)
							;

							if v_js_rowdes is not null then
							-- registro encontrado
								if v_js_rowori::varchar <> v_js_rowdes::varchar then
								-- registros diferentes
									-- generamos el update
									v_qr_actcam = '';

									for v_rs_coltip in
										select no_column, ti_column
										from becombas.tbtabori
										where id_compar = p_id_compar
										and no_esquem = v_rs_catori.no_esquem
										and no_tablas = v_rs_catori.no_tablas
										and va_prikey <> 'S'
										and no_column not ilike 'fe_%'
										and no_column <> 'us_regist'
										and no_column <> 'co_archiv'
										order by nu_ordcol
									loop
										if ((v_js_rowori ->> v_rs_coltip.no_column)::varchar <> (v_js_rowdes ->> v_rs_coltip.no_column)::varchar) or (v_js_rowdes ->> v_rs_coltip.no_column)::varchar is null then
											v_qr_actcam = v_qr_actcam || 
												v_rs_coltip.no_column || ' = ''' || replace((v_js_rowori ->> v_rs_coltip.no_column)::varchar, '''', '''''') || '''::' || v_rs_coltip.ti_column || ',';
										end if;
									end loop;

									if v_qr_actcam <> '' then
										v_qr_actcam = substring(v_qr_actcam, 1, length(v_qr_actcam) - 1);

										v_qr_return = v_qr_return || 'Update ' || v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas || chr(10) ||
											' Set ' || coalesce(v_qr_actcam, 'v_qr_actcam salio null') || chr(10);

										for v_rs_campos in
											select no_column, ti_column, nu_ordcol
											from becombas.tbtabori
											where id_compar = p_id_compar
											and no_esquem = v_rs_catori.no_esquem
											and no_tablas = v_rs_catori.no_tablas
											and va_prikey = 'S'
											order by nu_ordcol
										loop
											v_va_prikey = v_js_rowori ->> v_rs_campos.no_column;

											if v_rs_campos.nu_ordcol = 1 then
												v_qr_return = v_qr_return || 'where ' || (case when v_rs_campos.no_column is not null then v_rs_campos.no_column || ' = ''' || v_va_prikey || '''::' || v_rs_campos.ti_column else ' false' end);
											else
												v_qr_return = v_qr_return || (case when v_rs_campos.no_column is not null then ' and ' || v_rs_campos.no_column || ' = ''' || v_va_prikey || '''::' || v_rs_campos.ti_column else '' end);
											end if;
										end loop;

										v_qr_return = v_qr_return || ';' || chr(10) || chr(10);
									end if;
								end if;
							end if;
						end loop;
						
						if v_qr_insert_val <> '' then
							v_qr_return = v_qr_return || v_qr_insert || ';';
						end if;
					end if;
				end if;
			end if;

			if v_qr_return <> '' then
				v_rs_return.va_datori = v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas;
				v_rs_return.va_datdes = v_rs_catdes.no_esquem || '.' || v_rs_catdes.no_tablas;
				v_rs_return.va_sintax = v_qr_return;
				
				return next v_rs_return;
			end if;
		end loop;
	when 6 then
		-- REVISION DE FUNCIONES
		for v_rs_funori in
			select *
			from becombas.tbfunori
			where id_compar = p_id_compar
			--and no_esquem not in ('cpmotdec')
			order by no_esquem, no_funcio, ca_parfun
		loop
			v_qr_return = null;

			select * into v_rs_fundes
			from becombas.tbfundes
			where id_compar = p_id_compar
			and no_esquem = v_rs_funori.no_esquem
			and no_funcio = v_rs_funori.no_funcio
			and ca_parfun = v_rs_funori.ca_parfun
			and ls_parfun = v_rs_funori.ls_parfun
			and ls_partyp = v_rs_funori.ls_partyp;

			if v_rs_fundes.no_funcio is null then
				v_qr_return = v_rs_funori.df_funcio;
			elseif (v_rs_funori.ls_parfun <> v_rs_fundes.ls_parfun or 
				v_rs_funori.ls_partyp <> v_rs_fundes.ls_partyp or 
				replace(replace(v_rs_funori.df_funcio, '	', ''), ' ', '') <> replace(replace(v_rs_fundes.df_funcio, '	', ''), ' ', '')
			) then
				v_qr_return = v_rs_funori.df_funcio;
			end if;

			if v_qr_return is not null then
				v_rs_return.va_datori = v_rs_funori.no_esquem || '.' || v_rs_funori.no_funcio ||'('|| rtrim(ltrim(trim(v_rs_funori.ls_parfun), '{'), '}') ||')';
				v_rs_return.va_datdes = v_rs_funori.no_esquem || '.' || v_rs_fundes.no_funcio ||'('|| rtrim(ltrim(trim(v_rs_fundes.ls_parfun), '{'), '}') ||')';
				v_rs_return.va_sintax = v_qr_return || ';';

				return next v_rs_return;
			end if;
		end loop;
	end case;
	
	return;
end;
$$;


ALTER FUNCTION becombas.f_comparar_db_sintaxis_v3(p_id_compar integer, p_ti_compar integer) OWNER TO aguzman;

--
-- Name: f_comparar_db_sintaxis_v3_prueba(integer, integer); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_comparar_db_sintaxis_v3_prueba(p_id_compar integer, p_ti_compar integer) RETURNS SETOF becombas.pdcompar2
    LANGUAGE plpgsql
    AS $$
declare
	v_rs_tabori record;
	v_rs_tabdes record;
	v_rs_funori record;
	v_rs_fundes record;
	v_rs_catori record;
	v_rs_catdes record;
	v_rs_prikey record;
	v_rs_coltip record;
	v_rs_campos record;
	v_rs_return becombas.pdcompar2%rowtype;
	v_js_conori json;
	v_js_condes json;
	v_js_rowori json;
	v_js_rowdes json;
	v_nu_conori integer;
	v_nu_condes integer;
	v_ca_restos integer;
	v_ca_conori integer;
	v_ca_condes integer;
	v_co_bloque integer;
	v_no_esquem varchar;
	v_no_tablas varchar;
	v_qr_return varchar;
	v_va_coltip varchar;
	v_va_prike1 varchar;
	v_va_prike2 varchar;
	v_va_prike3 varchar;
	v_va_prike4 varchar;
	v_qr_insert varchar;
	v_qr_actcam varchar;
	v_va_prikey varchar;
	v_qr_prikey varchar;
	v_ls_bloque varchar;
	v_js_oritab varchar;
	v_js_oripar varchar;
	v_js_oriven varchar;
	v_js_orireg varchar;
	v_js_oribot varchar;
	v_js_oritit varchar;
	v_js_oriacc varchar;
	v_js_destab varchar;
	v_js_despar varchar;
	v_js_desven varchar;
	v_js_desreg varchar;
	v_js_desbot varchar;
	v_js_destit varchar;
	v_js_desacc varchar;
	v_ls_oriven varchar;
	v_ls_desven varchar;
	v_il_insert_cam boolean;
	v_qr_insert_cam varchar;
	v_qr_insert_val varchar;
	v_il_difblo boolean;
begin
	/*
		select * from becombas.f_comparar_db_sintaxis_v3(45, 1);
	*/
	-- REVISION DE ESTRUCTURAS
	case p_ti_compar
	when 2 then
	-- REVISION DE CONTENIDO DE CATALOGOS FRAMEWORK - INSERT FW
		select * into v_rs_catori
		from becombas.tbcatori
		where id_compar = p_id_compar
		and no_esquem = 'fwgescon'
		and no_tablas = 'tbbloque';
		
		select * into v_rs_catdes
		from becombas.tbcatdes
		where id_compar = p_id_compar
		and no_esquem = 'fwgescon'
		and no_tablas = 'tbbloque';
		
		v_js_conori := v_rs_catori.js_conten::json ->> 'ls_concat';
		v_js_condes := v_rs_catdes.js_conten::json ->> 'ls_concat';
		
		-- ARMAMOS LA SINTAXIS.
		if v_js_conori is not null then
			for v_js_rowori in
				select * from json_array_elements(v_js_conori::json)
			loop
				v_il_difblo := false;
				v_co_bloque = (v_js_rowori ->> 'co_bloque')::integer;
				
				select tx.js_rowdes into v_js_rowdes
				from (
					select value as js_rowdes
					from json_array_elements(v_js_condes::json)
				) tx
				where (tx.js_rowdes ->> 'co_bloque')::integer = v_co_bloque;
				
				if v_js_rowdes is not null then  -- bloque nuevo
					v_js_oritab := (
						select string_agg(js_rowori::varchar, ',')
						from (
							select js_rowori
							from json_array_elements((
								select js_conten::json -> 'ls_concat'
								from becombas.tbcatori
								where id_compar = p_id_compar
								and no_esquem = 'fwgescon'
								and no_tablas = 'tbblotab'
							)) js_rowori
						) tx
						where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
					);

					v_js_destab := (
						select string_agg(js_rowori::varchar, ',')
						from (
							select js_rowori
							from json_array_elements((
								select js_conten::json -> 'ls_concat'
								from becombas.tbcatdes
								where id_compar = p_id_compar
								and no_esquem = 'fwgescon'
								and no_tablas = 'tbblotab'
							)) js_rowori
						) tx
						where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
					);
					
					if (v_js_oritab = v_js_destab) or (v_js_oritab is null and v_js_destab is null) then
						v_js_oripar := (
							select string_agg(js_rowori::varchar, ',')
							from (
								select js_rowori
								from json_array_elements((
									select js_conten::json -> 'ls_concat'
									from becombas.tbcatori
									where id_compar = p_id_compar
									and no_esquem = 'fwgescon'
									and no_tablas = 'tbblopar'
								)) js_rowori
							) tx
							where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
						);
						
						v_js_despar := (
							select string_agg(js_rowori::varchar, ',')
							from (
								select js_rowori
								from json_array_elements(
									(select js_conten::json -> 'ls_concat'
									from becombas.tbcatdes
									where id_compar = p_id_compar
									and no_esquem = 'fwgescon'
									and no_tablas = 'tbblopar')
								) js_rowori
							) tx
							where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
						);
						
						if (v_js_oripar = v_js_despar) or (v_js_oripar is null and v_js_despar is null) then
							
							v_js_oriven := (
								select string_agg(js_rowori::varchar, ',')
								from (
									select js_rowori
									from json_array_elements(
										(select js_conten::json -> 'ls_concat'
										from becombas.tbcatori
										where id_compar = p_id_compar
										and no_esquem = 'fwgescon'
										and no_tablas = 'tbventan')
									) js_rowori
								) tx
								where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
							);
							
							v_js_desven := (
								select string_agg(js_rowori::varchar, ',')
								from (
									select js_rowori
									from json_array_elements(
										(select js_conten::json -> 'ls_concat'
										from becombas.tbcatdes
										where id_compar = p_id_compar
										and no_esquem = 'fwgescon'
										and no_tablas = 'tbventan')
									) js_rowori
								) tx
								where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
							);
							
							if (v_js_oriven = v_js_desven) or (v_js_oriven is null and v_js_desven is null) then
								v_js_oribot := (
									select string_agg(js_rowori::varchar, ',')
									from (
										select js_rowori
										from json_array_elements(
											(select js_conten::json -> 'ls_concat'
											from becombas.tbcatori
											where id_compar = p_id_compar
											and no_esquem = 'fwgescon'
											and no_tablas = 'tbvenbot')
										) js_rowori
									) tx
									where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
								);

								v_js_desbot := (
									select string_agg(js_rowori::varchar, ',')
									from (
										select js_rowori
										from json_array_elements(
											(select js_conten::json -> 'ls_concat'
											from becombas.tbcatdes
											where id_compar = p_id_compar
											and no_esquem = 'fwgescon'
											and no_tablas = 'tbvenbot')
										) js_rowori
									) tx
									where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
								);
								
								if (v_js_oribot = v_js_desbot) or (v_js_oribot is null and v_js_desbot is null) then
									v_js_orireg := (
										select string_agg(js_rowori::varchar, ',')
										from (
											select js_rowori
											from json_array_elements(
												(select js_conten::json -> 'ls_concat'
												from becombas.tbcatori
												where id_compar = p_id_compar
												and no_esquem = 'fwgescon'
												and no_tablas = 'tbvenreg')
											) js_rowori
										) tx
										where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
									);

									v_js_desreg := (
										select string_agg(js_rowori::varchar, ',')
										from (
											select js_rowori
											from json_array_elements(
												(select js_conten::json -> 'ls_concat'
												from becombas.tbcatdes
												where id_compar = p_id_compar
												and no_esquem = 'fwgescon'
												and no_tablas = 'tbvenreg')
											) js_rowori
										) tx
										where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
									);
									
									if (v_js_orireg = v_js_desreg) or (v_js_orireg is null and v_js_desreg is null) then
										v_ls_oriven := (
											select string_agg(tx.js_rowori ->> 'co_ventan', ',')
											from (
												select js_rowori
												from json_array_elements((
													select js_conten::json -> 'ls_concat'
													from becombas.tbcatori
													where id_compar = p_id_compar
													and no_esquem = 'fwgescon'
													and no_tablas = 'tbventan'
												)) js_rowori
											) tx
											where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
										);

										v_ls_desven := (
											select string_agg(tx.js_rowori ->> 'co_ventan', ',')
											from (
												select js_rowori
												from json_array_elements((
													select js_conten::json -> 'ls_concat'
													from becombas.tbcatdes
													where id_compar = p_id_compar
													and no_esquem = 'fwgescon'
													and no_tablas = 'tbventan'
												)) js_rowori
											) tx
											where (tx.js_rowori ->> 'co_bloque')::integer = v_co_bloque
										);

										v_js_oritit := (
											select string_agg(js_rowori::varchar, ',')
											from (
												select js_rowori
												from json_array_elements(
													(select js_conten::json -> 'ls_concat'
													from becombas.tbcatori
													where id_compar = p_id_compar
													and no_esquem = 'fwgescon'
													and no_tablas = 'tbventit')
												) js_rowori
											) tx
											where (tx.js_rowori ->> 'co_ventan')::integer in (
												select co_ventan::integer 
												from regexp_split_to_table(v_ls_oriven, ',') as co_ventan
											)
										);

										v_js_destit := (
											select string_agg(js_rowori::varchar, ',')
											from (
												select js_rowori
												from json_array_elements(
													(select js_conten::json -> 'ls_concat'
													from becombas.tbcatdes
													where id_compar = p_id_compar
													and no_esquem = 'fwgescon'
													and no_tablas = 'tbventit')
												) js_rowori
											) tx
											where (tx.js_rowori ->> 'co_ventan')::integer in (
												select co_ventan::integer 
												from regexp_split_to_table(v_ls_desven, ',') as co_ventan
											)
										);
										
										if (v_js_oritit = v_js_destit) or (v_js_oritit is null and v_js_destit is null) then
											v_js_oriacc := (
												select string_agg(js_rowori::varchar, ',')
												from (
													select js_rowori
													from json_array_elements(
														(select js_conten::json -> 'ls_concat'
														from becombas.tbcatori
														where id_compar = p_id_compar
														and no_esquem = 'fwgescon'
														and no_tablas = 'tbbotacc')
													) js_rowori
												) tx
												where (tx.js_rowori ->> 'co_ventan')::integer in (
													select co_ventan::integer 
													from regexp_split_to_table(v_ls_oriven, ',') as co_ventan
												) 
											);

											v_js_desacc := (
												select string_agg(js_rowori::varchar, ',')
												from (
													select js_rowori
													from json_array_elements(
														(select js_conten::json -> 'ls_concat'
														from becombas.tbcatdes
														where id_compar = p_id_compar
														and no_esquem = 'fwgescon'
														and no_tablas = 'tbbotacc')
													) js_rowori
												) tx
												where (tx.js_rowori ->> 'co_ventan')::integer in (
													select co_ventan::integer 
													from regexp_split_to_table(v_ls_desven, ',') as co_ventan
												) 
											);
											
											if (v_js_oriacc = v_js_desacc) or (v_js_oriacc is null and v_js_desacc is null) then
											else
												raise notice 'ACCIONES:
v_js_oriacc %
v_js_desacc %', v_js_oriacc, v_js_desacc;
												v_il_difblo := true;
											end if;
										else
											raise notice 'TITULOS:
v_js_oritit %
v_js_destit %', v_js_oritit, v_js_destit;
											v_il_difblo := true;
										end if;
									else
										raise notice 'REGISTROS: 
v_js_orireg %
v_js_desreg %', v_js_orireg, v_js_desreg;
										v_il_difblo := true;
									end if;
								else
									raise notice 'BOTONES: 
v_js_oribot %
v_js_desbot %', v_js_oribot, v_js_desbot;
									v_il_difblo := true;
								end if;
							else
								raise notice 'VENTANAS:
v_js_oriven %
v_js_desven %', v_js_oriven, v_js_desven;
								v_il_difblo := true;
							end if;
						else
							raise notice 'PARAMETROS:
v_js_oripar %
v_js_despar %', v_js_oripar, v_js_despar;
							v_il_difblo := true;
						end if;
					else
						raise notice 'TABS:
v_js_oritab %
v_js_destab %', v_js_oritab, v_js_destab;
						v_il_difblo := true;
					end if;
				else
					raise notice 'FILA DES NULL:
v_js_rowori %
v_js_rowdes %', v_js_rowori, v_js_rowdes;
					v_il_difblo := true;
				end if;
				
				if v_il_difblo then
					v_ls_bloque := (case when v_ls_bloque is null then v_co_bloque::varchar else v_ls_bloque || ',' || v_co_bloque::varchar end);
				end if;
			end loop;
		end if;
		
		v_rs_return.va_datori = v_rs_catori.no_esquem || '.' || v_rs_catori.no_tablas;
		v_rs_return.va_datdes = v_rs_catdes.no_esquem || '.' || v_rs_catdes.no_tablas;
		v_rs_return.va_sintax = v_ls_bloque;

		return next v_rs_return;
		end case;
	return;
end;
$$;


ALTER FUNCTION becombas.f_comparar_db_sintaxis_v3_prueba(p_id_compar integer, p_ti_compar integer) OWNER TO aguzman;

--
-- Name: f_de_basdes(integer); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_de_basdes(p_id_compar integer) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
declare
begin
	return (
      select de_basdes
      from becombas.tbcompar
      where id_compar = p_id_compar
   );
end;
$$;


ALTER FUNCTION becombas.f_de_basdes(p_id_compar integer) OWNER TO aguzman;

--
-- Name: f_de_basori(integer); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_de_basori(p_id_compar integer) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
declare
begin
	return (
      select de_basori
      from becombas.tbcompar
      where id_compar = p_id_compar
   );
end;
$$;


ALTER FUNCTION becombas.f_de_basori(p_id_compar integer) OWNER TO aguzman;

--
-- Name: f_de_compar(integer); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_de_compar(p_id_compar integer) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
declare
begin
	return (
      select de_compar
      from becombas.tbcompar
      where id_compar = p_id_compar
   );
end;
$$;


ALTER FUNCTION becombas.f_de_compar(p_id_compar integer) OWNER TO aguzman;

--
-- Name: f_id_compar(character varying, character varying, character varying, character varying, character varying); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_id_compar(p_de_compar character varying, p_no_basori character varying, p_de_basori character varying, p_no_basdes character varying, p_de_basdes character varying) RETURNS integer
    LANGUAGE plpgsql
    AS $$
declare
	v_id_compar integer;
begin
	v_id_compar := becombas.f_id_compar(
      p_de_compar,
      p_no_basori,
      p_de_basori,
      p_no_basdes,
      p_de_basdes,
      null::integer
   );
	
	return v_id_compar;
end;
$$;


ALTER FUNCTION becombas.f_id_compar(p_de_compar character varying, p_no_basori character varying, p_de_basori character varying, p_no_basdes character varying, p_de_basdes character varying) OWNER TO aguzman;

--
-- Name: f_id_compar(character varying, character varying, character varying, character varying, character varying, integer); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_id_compar(p_de_compar character varying, p_no_basori character varying, p_de_basori character varying, p_no_basdes character varying, p_de_basdes character varying, p_id_comant integer) RETURNS integer
    LANGUAGE plpgsql
    AS $$
declare
	v_id_compar integer;
   v_no_basdes varchar;
begin
	insert into becombas.tbcompar (de_compar, no_basori, de_basori, no_basdes, de_basdes)
	values (p_de_compar, p_no_basori, p_de_basori, p_no_basdes, p_de_basdes)
	returning id_compar into v_id_compar;
   
   v_no_basdes := becombas.f_no_basori(p_id_comant);
   
   if v_id_compar is not null and
      (p_no_basdes = v_no_basdes)
   then
      raise notice 'Inicializando la base de destino';
      
      insert into becombas.tbtabdes (
         id_compar,
         nu_compar,
         no_esquem,
         no_tablas,
         nu_ordcol,
         no_column,
         ti_column,
         va_defaul,
         va_notnul,
         va_prikey,
         va_forkey,
         de_coment
      ) select
         v_id_compar,
         nu_compar,
         no_esquem,
         no_tablas,
         nu_ordcol,
         no_column,
         ti_column,
         va_defaul,
         va_notnul,
         va_prikey,
         va_forkey,
         de_coment
      from becombas.tbtabori
      where id_compar in (p_id_comant);

      insert into becombas.tbcatdes(
         id_compar,
         nu_compar,
         no_esquem,
         no_tablas,
         js_conten
      )
      select
         v_id_compar,
         nu_compar,
         no_esquem,
         no_tablas,
         js_conten
      from becombas.tbcatori
      where id_compar in (p_id_comant);

      insert into becombas.tbfundes (
         id_compar,
         nu_compar,
         no_esquem,
         no_funcio,
         de_coment,
         ca_parfun,
         ls_parfun,
         df_funcio,
         ls_partyp
      )
      select
         v_id_compar,
         nu_compar,
         no_esquem,
         no_funcio,
         de_coment,
         ca_parfun,
         ls_parfun,
         df_funcio,
         ls_partyp
      from becombas.tbfunori
      where id_compar in (p_id_comant);

      insert into becombas.tbobjdes(
         id_compar,
         nu_compar,
         ti_objeto,
         no_esquem,
         no_objeto,
         de_coment,
         js_objeto
      )
      select
         v_id_compar,
         nu_compar,
         ti_objeto,
         no_esquem,
         no_objeto,
         de_coment,
         js_objeto
      from becombas.tbobjori
      where id_compar in (p_id_comant);
	end if;
   
	return v_id_compar;
end;
$$;


ALTER FUNCTION becombas.f_id_compar(p_de_compar character varying, p_no_basori character varying, p_de_basori character varying, p_no_basdes character varying, p_de_basdes character varying, p_id_comant integer) OWNER TO aguzman;

--
-- Name: f_id_compar(character varying, character varying, character varying, character varying, character varying, integer, integer); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_id_compar(p_de_compar character varying, p_no_basori character varying, p_de_basori character varying, p_no_basdes character varying, p_de_basdes character varying, p_id_comori integer, p_id_comdes integer) RETURNS integer
    LANGUAGE plpgsql
    AS $$
declare
	v_id_compar integer;
   v_no_basdes varchar;
   v_no_basori varchar;
begin
	insert into becombas.tbcompar (de_compar, no_basori, de_basori, no_basdes, de_basdes)
	values (p_de_compar, p_no_basori, p_de_basori, p_no_basdes, p_de_basdes)
	returning id_compar into v_id_compar;
   
   v_no_basdes := becombas.f_no_basori(p_id_comdes);
   
   if v_id_compar is not null and
      (p_no_basdes = v_no_basdes)
   then
      raise notice 'Inicializando la base de destino';
      
      insert into becombas.tbtabdes (
         id_compar,
         nu_compar,
         no_esquem,
         no_tablas,
         nu_ordcol,
         no_column,
         ti_column,
         va_defaul,
         va_notnul,
         va_prikey,
         va_forkey,
         de_coment
      ) select
         v_id_compar,
         nu_compar,
         no_esquem,
         no_tablas,
         nu_ordcol,
         no_column,
         ti_column,
         va_defaul,
         va_notnul,
         va_prikey,
         va_forkey,
         de_coment
      from becombas.tbtabori
      where id_compar in (p_id_comdes);

      insert into becombas.tbcatdes(
         id_compar,
         nu_compar,
         no_esquem,
         no_tablas,
         js_conten
      )
      select
         v_id_compar,
         nu_compar,
         no_esquem,
         no_tablas,
         js_conten
      from becombas.tbcatori
      where id_compar in (p_id_comdes);

      insert into becombas.tbfundes (
         id_compar,
         nu_compar,
         no_esquem,
         no_funcio,
         de_coment,
         ca_parfun,
         ls_parfun,
         df_funcio,
         ls_partyp
      )
      select
         v_id_compar,
         nu_compar,
         no_esquem,
         no_funcio,
         de_coment,
         ca_parfun,
         ls_parfun,
         df_funcio,
         ls_partyp
      from becombas.tbfunori
      where id_compar in (p_id_comdes);

      insert into becombas.tbobjdes(
         id_compar,
         nu_compar,
         ti_objeto,
         no_esquem,
         no_objeto,
         de_coment,
         js_objeto
      )
      select
         v_id_compar,
         nu_compar,
         ti_objeto,
         no_esquem,
         no_objeto,
         de_coment,
         js_objeto
      from becombas.tbobjori
      where id_compar in (p_id_comdes);
	end if;
   
   
   v_no_basori := becombas.f_no_basori(p_id_comori);
   
   if v_id_compar is not null and
      (p_no_basori = v_no_basori)
   then
      raise notice 'Inicializando la base de origen';
      
      insert into becombas.tbtabori (
         id_compar,
         nu_compar,
         no_esquem,
         no_tablas,
         nu_ordcol,
         no_column,
         ti_column,
         va_defaul,
         va_notnul,
         va_prikey,
         va_forkey,
         de_coment
      ) select
         v_id_compar,
         nu_compar,
         no_esquem,
         no_tablas,
         nu_ordcol,
         no_column,
         ti_column,
         va_defaul,
         va_notnul,
         va_prikey,
         va_forkey,
         de_coment
      from becombas.tbtabori
      where id_compar in (p_id_comori);

      insert into becombas.tbcatori(
         id_compar,
         nu_compar,
         no_esquem,
         no_tablas,
         js_conten
      )
      select
         v_id_compar,
         nu_compar,
         no_esquem,
         no_tablas,
         js_conten
      from becombas.tbcatori
      where id_compar in (p_id_comori);

      insert into becombas.tbfunori (
         id_compar,
         nu_compar,
         no_esquem,
         no_funcio,
         de_coment,
         ca_parfun,
         ls_parfun,
         df_funcio,
         ls_partyp
      )
      select
         v_id_compar,
         nu_compar,
         no_esquem,
         no_funcio,
         de_coment,
         ca_parfun,
         ls_parfun,
         df_funcio,
         ls_partyp
      from becombas.tbfunori
      where id_compar in (p_id_comori);

      insert into becombas.tbobjori(
         id_compar,
         nu_compar,
         ti_objeto,
         no_esquem,
         no_objeto,
         de_coment,
         js_objeto
      )
      select
         v_id_compar,
         nu_compar,
         ti_objeto,
         no_esquem,
         no_objeto,
         de_coment,
         js_objeto
      from becombas.tbobjori
      where id_compar in (p_id_comori);
	end if;
   
	return v_id_compar;
end;
$$;


ALTER FUNCTION becombas.f_id_compar(p_de_compar character varying, p_no_basori character varying, p_de_basori character varying, p_no_basdes character varying, p_de_basdes character varying, p_id_comori integer, p_id_comdes integer) OWNER TO aguzman;

--
-- Name: f_insert_compar(integer, integer, integer); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_insert_compar(p_id_compar integer, p_id_comori integer, p_id_comdes integer) RETURNS integer
    LANGUAGE plpgsql
    AS $$
declare
   v_no_basdes varchar;
   v_no_basori varchar;
begin
   v_no_basdes := becombas.f_no_basori(p_id_comdes);
   
   if becombas.f_de_compar(p_id_compar) is null then
      return null;
   end if;
   
   if p_id_compar is not null and
      v_no_basdes is not null
   then
      raise notice 'Inicializando la base de destino';
      
      insert into becombas.tbtabdes (
         id_compar,
         nu_compar,
         no_esquem,
         no_tablas,
         nu_ordcol,
         no_column,
         ti_column,
         va_defaul,
         va_notnul,
         va_prikey,
         va_forkey,
         de_coment
      ) select
         p_id_compar,
         nu_compar,
         no_esquem,
         no_tablas,
         nu_ordcol,
         no_column,
         ti_column,
         va_defaul,
         va_notnul,
         va_prikey,
         va_forkey,
         de_coment
      from becombas.tbtabori
      where id_compar in (p_id_comdes);

      insert into becombas.tbcatdes(
         id_compar,
         nu_compar,
         no_esquem,
         no_tablas,
         js_conten
      )
      select
         p_id_compar,
         nu_compar,
         no_esquem,
         no_tablas,
         js_conten
      from becombas.tbcatori
      where id_compar in (p_id_comdes);

      insert into becombas.tbfundes (
         id_compar,
         nu_compar,
         no_esquem,
         no_funcio,
         de_coment,
         ca_parfun,
         ls_parfun,
         df_funcio,
         ls_partyp
      )
      select
         p_id_compar,
         nu_compar,
         no_esquem,
         no_funcio,
         de_coment,
         ca_parfun,
         ls_parfun,
         df_funcio,
         ls_partyp
      from becombas.tbfunori
      where id_compar in (p_id_comdes);

      insert into becombas.tbobjdes(
         id_compar,
         nu_compar,
         ti_objeto,
         no_esquem,
         no_objeto,
         de_coment,
         js_objeto
      )
      select
         p_id_compar,
         nu_compar,
         ti_objeto,
         no_esquem,
         no_objeto,
         de_coment,
         js_objeto
      from becombas.tbobjori
      where id_compar in (p_id_comdes);
	end if;
   
   v_no_basori := becombas.f_no_basori(p_id_comori);
   
   if p_id_compar is not null and
      v_no_basori is not null
   then
      raise notice 'Inicializando la base de origen';
      
      insert into becombas.tbtabori (
         id_compar,
         nu_compar,
         no_esquem,
         no_tablas,
         nu_ordcol,
         no_column,
         ti_column,
         va_defaul,
         va_notnul,
         va_prikey,
         va_forkey,
         de_coment
      ) select
         p_id_compar,
         nu_compar,
         no_esquem,
         no_tablas,
         nu_ordcol,
         no_column,
         ti_column,
         va_defaul,
         va_notnul,
         va_prikey,
         va_forkey,
         de_coment
      from becombas.tbtabori
      where id_compar in (p_id_comori);

      insert into becombas.tbcatori(
         id_compar,
         nu_compar,
         no_esquem,
         no_tablas,
         js_conten
      )
      select
         p_id_compar,
         nu_compar,
         no_esquem,
         no_tablas,
         js_conten
      from becombas.tbcatori
      where id_compar in (p_id_comori);

      insert into becombas.tbfunori (
         id_compar,
         nu_compar,
         no_esquem,
         no_funcio,
         de_coment,
         ca_parfun,
         ls_parfun,
         df_funcio,
         ls_partyp
      )
      select
         p_id_compar,
         nu_compar,
         no_esquem,
         no_funcio,
         de_coment,
         ca_parfun,
         ls_parfun,
         df_funcio,
         ls_partyp
      from becombas.tbfunori
      where id_compar in (p_id_comori);

      insert into becombas.tbobjori(
         id_compar,
         nu_compar,
         ti_objeto,
         no_esquem,
         no_objeto,
         de_coment,
         js_objeto
      )
      select
         p_id_compar,
         nu_compar,
         ti_objeto,
         no_esquem,
         no_objeto,
         de_coment,
         js_objeto
      from becombas.tbobjori
      where id_compar in (p_id_comori);
	end if;
   
	return p_id_compar;
end;
$$;


ALTER FUNCTION becombas.f_insert_compar(p_id_compar integer, p_id_comori integer, p_id_comdes integer) OWNER TO aguzman;

--
-- Name: f_no_basdes(integer); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_no_basdes(p_id_compar integer) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
declare
begin
	return (
      select no_basdes
      from becombas.tbcompar
      where id_compar = p_id_compar
   );
end;
$$;


ALTER FUNCTION becombas.f_no_basdes(p_id_compar integer) OWNER TO aguzman;

--
-- Name: f_no_basori(integer); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_no_basori(p_id_compar integer) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
declare
begin
	return (
      select no_basori
      from becombas.tbcompar
      where id_compar = p_id_compar
   );
end;
$$;


ALTER FUNCTION becombas.f_no_basori(p_id_compar integer) OWNER TO aguzman;

--
-- Name: f_proceso_comparacion(integer); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_proceso_comparacion(p_id_compar integer) RETURNS SETOF becombas.pdcompar3
    LANGUAGE plpgsql
    AS $$
declare
	v_qr_inform varchar;
	v_ls_compar varchar;
	v_rs_proces record;
	v_nu_compar integer;
begin
	--  eliminando el contenido del paquete de la ultima comparacion
	raise notice 'Eliminando la ultima foto anterior becombas.f_actualizar_db(3, %)', p_id_compar;
	perform becombas.f_actualizar_db(3, p_id_compar);
	
	-- Proceso de comparacion de versiones (comparar fotos: inicio y fin)
	v_nu_compar := 0;
	
	-- PROCESO DE COMPARACION DE ESTRUCTURAS
	raise notice 'insertando los cambios de estructura: becombas.f_comparar_db_sintaxis_db_central_v1(%, 1)', p_id_compar;
	
	for v_rs_proces in
		select ''::varchar as va_datcon, *
      from becombas.f_comparar_db_sintaxis_db_v4(p_id_compar, 1)
	loop
		v_nu_compar := v_nu_compar + 1;
		
		insert into becombas.tbpaquet (id_compar, nu_compar, no_objeto, qr_objeto, ti_paquet)
		values (p_id_compar, v_nu_compar, v_rs_proces.va_datori, v_rs_proces.va_sintax, 1);
		
		if exists(
			select *
			from becombas.tbpaquet
			where fe_paspaq is null
			and no_objeto = v_rs_proces.va_datori
			and qr_objeto <> v_rs_proces.va_sintax
			and id_compar <> p_id_compar
			and ti_paquet = 1
		) then
			v_ls_compar := (
				select string_agg(id_compar::varchar, ',')
				from becombas.tbpaquet
				where fe_paspaq is null
				and no_objeto = v_rs_proces.va_datori
				and qr_objeto <> v_rs_proces.va_sintax
				and id_compar <> p_id_compar
				and ti_paquet = 1
			);
			
			v_rs_proces.va_datcon := ('** CONFLICT ** id_compar: ' || v_ls_compar)::varchar; 
		end if;
		
		return next v_rs_proces;
	end loop;
	
	-- PROCESO DE COMPARACION DE INSERT WF
	raise notice 'insertando los cambios de insert de los catagolos del WF a la tabla de paquete: f_comparar_db_sintaxis_db_central_v1(%, 4)', p_id_compar;
	for v_rs_proces in
		select ''::varchar as va_datcon, *
      from becombas.f_comparar_db_sintaxis_db_v4(p_id_compar, 4)
	loop
		v_nu_compar := v_nu_compar + 1;
		
		insert into becombas.tbpaquet (id_compar, nu_compar, no_objeto, qr_objeto, ti_paquet)
		values (p_id_compar, v_nu_compar, v_rs_proces.va_datori, v_rs_proces.va_sintax, 4);
		
		if exists(
			select *
			from becombas.tbpaquet
			where fe_paspaq is null
			and no_objeto = v_rs_proces.va_datori
			and qr_objeto <> v_rs_proces.va_sintax
			and id_compar <> p_id_compar
			and ti_paquet = 4
		) then
			v_ls_compar := (
				select string_agg(id_compar::varchar, ',')
				from becombas.tbpaquet
				where fe_paspaq is null
				and no_objeto = v_rs_proces.va_datori
				and qr_objeto <> v_rs_proces.va_sintax
				and id_compar <> p_id_compar
				and ti_paquet = 4
			);
			
			v_rs_proces.va_datcon = ('** CONFLICT ** id_compar: ' || v_ls_compar)::varchar; 
		end if;
		
		return next v_rs_proces;
	end loop;
	
	-- PROCESO DE COMPARACION DE UPDATE WF
	raise notice 'insertando los cambios de update de los catagolos del WF a la tabla de paquete: f_comparar_db_sintaxis_db_central_v1(%, 5)', p_id_compar;
	for v_rs_proces in
		select ''::varchar as va_datcon, *
      from becombas.f_comparar_db_sintaxis_db_v4(p_id_compar, 5)
	loop
		v_nu_compar := v_nu_compar + 1;
		
		insert into becombas.tbpaquet (id_compar, nu_compar, no_objeto, qr_objeto, ti_paquet)
		values (p_id_compar, v_nu_compar, v_rs_proces.va_datori, v_rs_proces.va_sintax, 5);
		
		if exists(
			select *
			from becombas.tbpaquet
			where fe_paspaq is null
			and no_objeto = v_rs_proces.va_datori
			and qr_objeto <> v_rs_proces.va_sintax
			and id_compar <> p_id_compar
			and ti_paquet = 5
		) then
			v_ls_compar := (
				select string_agg(id_compar::varchar, ',')
				from becombas.tbpaquet
				where fe_paspaq is null
				and no_objeto = v_rs_proces.va_datori
				and qr_objeto <> v_rs_proces.va_sintax
				and id_compar <> p_id_compar
				and ti_paquet = 5
			);
			
			v_rs_proces.va_datcon = ('** CONFLICT ** id_compar: ' || v_ls_compar)::varchar; 
		end if;
		
		return next v_rs_proces;
	end loop;
	
	-- PROCESO DE COMPARACION DE FUNCIONES (fwlogneg y public)
	raise notice 'Proceso de comparacion de funciones: becombas.f_comparar_db_sintaxis_db_central_v1(%, 6)', p_id_compar;
	
	for v_rs_proces in
		select ''::varchar as va_datcon, *
      from becombas.f_comparar_db_sintaxis_db_v4(p_id_compar, 6)
	loop
		v_nu_compar := v_nu_compar + 1;
		
		--v_rs_proces.va_sintax := becombas.f_variables_limpiar(v_rs_proces.va_sintax);
		
		insert into becombas.tbpaquet (id_compar, nu_compar, no_objeto, qr_objeto, ti_paquet)
		values (p_id_compar, v_nu_compar, v_rs_proces.va_datori, v_rs_proces.va_sintax, 6);
		/*
		if exists(
			select *
			from becombas.tbpaquet
			where fe_paspaq is null
			and no_objeto = v_rs_proces.va_datori
			and qr_objeto <> v_rs_proces.va_sintax
			and id_compar <> p_id_compar
			and ti_paquet = 6
		) then
			v_ls_compar := (
				select string_agg(id_compar::varchar, ',')
				from becombas.tbpaquet
				where fe_paspaq is null
				and no_objeto = v_rs_proces.va_datori
				and qr_objeto <> v_rs_proces.va_sintax
				and id_compar <> p_id_compar
				and ti_paquet = 6
			);
			
			v_rs_proces.va_datcon = ('** CONFLICT ** id_compar: ' || v_ls_compar)::varchar; 
		end if;
      */
		
		return next v_rs_proces;
	end loop;
	
	return;
end;
$$;


ALTER FUNCTION becombas.f_proceso_comparacion(p_id_compar integer) OWNER TO aguzman;

--
-- Name: f_push_base_central(); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_push_base_central() RETURNS SETOF becombas.pdcompar3
    LANGUAGE plpgsql
    AS $$
declare
	v_qr_inform varchar;
	v_ls_compar varchar;
	v_rs_proces record;
	v_nu_compar integer;
begin
	-- eliminando algun push anterior de la instancia origen (que es la instancia que tiene los cambios trabajados)
	raise notice 'Eliminando la ultima foto anterior becombas.f_actualizar_db(138)';
	perform becombas.f_actualizar_db(138);
	
	-- Obtener informacion de la base de datos (foto de cambios)
	raise notice 'Obtener informacion de la base de datos (foto de cambios): f_comparar_db_sintaxis_db_central_v1(1, 138)';
	v_qr_inform := becombas.f_comparar_db_v2(1, 138);
	
	-- Procesar informacion
	execute(v_qr_inform);
	
	-- Proceso de comparacion de versiones (comparar fotos: inicio y fin)
	v_nu_compar := 0;
	
	-- PROCESO DE COMPARACION DE ESTRUCTURAS
	raise notice 'insertando los cambios de estructura: becombas.f_comparar_db_sintaxis_db_central_v1(138, 1)';
	
	for v_rs_proces in
		select ''::varchar as va_datcon, *
      from becombas.f_comparar_db_sintaxis_db_central_v1(138, 1)
	loop
		v_nu_compar := v_nu_compar + 1;
		
		insert into becombas.tbpaquet (id_compar, nu_compar, no_objeto, qr_objeto, ti_paquet)
		values (138, v_nu_compar, v_rs_proces.va_datori, v_rs_proces.va_sintax, 1);
		
		if exists(
			select *
			from becombas.tbpaquet
			where fe_paspaq is null
			and no_objeto = v_rs_proces.va_datori
			and qr_objeto <> v_rs_proces.va_sintax
			and id_compar <> 138
			and ti_paquet = 1
		) then
			v_ls_compar := (
				select string_agg(id_compar::varchar, ',')
				from becombas.tbpaquet
				where fe_paspaq is null
				and no_objeto = v_rs_proces.va_datori
				and qr_objeto <> v_rs_proces.va_sintax
				and id_compar <> 138
				and ti_paquet = 1
			);
			
			v_rs_proces.va_datcon = ('** CONFLICT ** id_compar: ' || v_ls_compar)::varchar; 
		end if;
		
		return next v_rs_proces;
	end loop;
	
	-- PROCESO DE COMPARACION DE INSERT WF
	raise notice 'insertando los cambios de insert de los catagolos del WF a la tabla de paquete: f_comparar_db_sintaxis_db_central_v1(138, 4)';
	for v_rs_proces in
		select ''::varchar as va_datcon, *
      from becombas.f_comparar_db_sintaxis_db_central_v1(138, 4)
	loop
		v_nu_compar := v_nu_compar + 1;
		
		insert into becombas.tbpaquet (id_compar, nu_compar, no_objeto, qr_objeto, ti_paquet)
		values (138, v_nu_compar, v_rs_proces.va_datori, v_rs_proces.va_sintax, 4);
		
		if exists(
			select *
			from becombas.tbpaquet
			where fe_paspaq is null
			and no_objeto = v_rs_proces.va_datori
			and qr_objeto <> v_rs_proces.va_sintax
			and id_compar <> 138
			and ti_paquet = 4
		) then
			v_ls_compar := (
				select string_agg(id_compar::varchar, ',')
				from becombas.tbpaquet
				where fe_paspaq is null
				and no_objeto = v_rs_proces.va_datori
				and qr_objeto <> v_rs_proces.va_sintax
				and id_compar <> 138
				and ti_paquet = 4
			);
			
			v_rs_proces.va_datcon = ('** CONFLICT ** id_compar: ' || v_ls_compar)::varchar; 
		end if;
		
		return next v_rs_proces;
	end loop;
	
	-- PROCESO DE COMPARACION DE UPDATE WF
	raise notice 'insertando los cambios de update de los catagolos del WF a la tabla de paquete: f_comparar_db_sintaxis_db_central_v1(138, 5)';
	for v_rs_proces in
		select ''::varchar as va_datcon, *
      from becombas.f_comparar_db_sintaxis_db_central_v1(138, 5)
	loop
		v_nu_compar := v_nu_compar + 1;
		
		insert into becombas.tbpaquet (id_compar, nu_compar, no_objeto, qr_objeto, ti_paquet)
		values (138, v_nu_compar, v_rs_proces.va_datori, v_rs_proces.va_sintax, 5);
		
		if exists(
			select *
			from becombas.tbpaquet
			where fe_paspaq is null
			and no_objeto = v_rs_proces.va_datori
			and qr_objeto <> v_rs_proces.va_sintax
			and id_compar <> 138
			and ti_paquet = 5
		) then
			v_ls_compar := (
				select string_agg(id_compar::varchar, ',')
				from becombas.tbpaquet
				where fe_paspaq is null
				and no_objeto = v_rs_proces.va_datori
				and qr_objeto <> v_rs_proces.va_sintax
				and id_compar <> 138
				and ti_paquet = 5
			);
			
			v_rs_proces.va_datcon = ('** CONFLICT ** id_compar: ' || v_ls_compar)::varchar; 
		end if;
		
		return next v_rs_proces;
	end loop;
	
	-- PROCESO DE COMPARACION DE FUNCIONES (fwlogneg y public)
	raise notice 'Proceso de comparacion de funciones: becombas.f_comparar_db_sintaxis_db_central_v1(138, 6)';
	
	for v_rs_proces in
		select ''::varchar as va_datcon, *
      from becombas.f_comparar_db_sintaxis_db_central_v1(138, 6)
	loop
		v_nu_compar := v_nu_compar + 1;
		
		--v_rs_proces.va_sintax := becombas.f_variables_limpiar(v_rs_proces.va_sintax);
		
		insert into becombas.tbpaquet (id_compar, nu_compar, no_objeto, qr_objeto, ti_paquet)
		values (138, v_nu_compar, v_rs_proces.va_datori, v_rs_proces.va_sintax, 6);
		
		if exists(
			select *
			from becombas.tbpaquet
			where fe_paspaq is null
			and no_objeto = v_rs_proces.va_datori
			and qr_objeto <> v_rs_proces.va_sintax
			and id_compar <> 138
			and ti_paquet = 6
		) then
			v_ls_compar := (
				select string_agg(id_compar::varchar, ',')
				from becombas.tbpaquet
				where fe_paspaq is null
				and no_objeto = v_rs_proces.va_datori
				and qr_objeto <> v_rs_proces.va_sintax
				and id_compar <> 138
				and ti_paquet = 6
			);
			
			v_rs_proces.va_datcon = ('** CONFLICT ** id_compar: ' || v_ls_compar)::varchar; 
		end if;
		
		return next v_rs_proces;
	end loop;
	
	return;
end;
$$;


ALTER FUNCTION becombas.f_push_base_central() OWNER TO aguzman;

--
-- Name: f_push_proyecto(); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_push_proyecto() RETURNS SETOF becombas.pdcompar3
    LANGUAGE plpgsql
    AS $$
declare
	v_qr_return varchar := '';
	v_qr_deltra varchar := '';
	v_qr_bloque varchar := '';
	v_qr_update varchar := '';
	v_va_return varchar;
	v_ls_bloque varchar;
	v_qr_inform varchar;
	v_ls_compar varchar;
	v_rs_return record;
	v_rs_proces record;
	v_rs_bloque record;
	v_co_blomax integer;
	v_nu_compar integer;
begin
	-- eliminando algun push anterior de la instancia origen (que es la instancia que tiene los cambios trabajados)
	raise notice 'Eliminando la ultima foto anterior becombas.f_actualizar_db(139)';
	perform becombas.f_actualizar_db(139);
	
	-- Obtener informacion de la base de datos (foto de cambios)
	raise notice 'Obtener informacion de la base de datos (foto de cambios): f_comparar_db_v2(1, 139)';
	v_qr_inform := becombas.f_comparar_db_v2(1, 139);
	
	-- Procesar informacion
	execute(v_qr_inform);
	
	-- Proceso de comparacion de versiones (comparar fotos: inicio y fin)
	raise notice 'Lista de Bloques que cambiaron: becombas.f_comparar_db_sintaxis_v4(139, 2)';
	v_ls_bloque := (select va_sintax from becombas.f_comparar_db_sintaxis_v4(139, 2));
	
	-- codigo de bloque maximo de la primera foto
	v_co_blomax := (
		select max((js_bloque ->> 'co_bloque')::integer)
		from json_array_elements((
			select js_conten::json -> 'ls_concat'
			from becombas.tbcatdes
			where id_compar = 139
			and no_esquem = 'fwgescon'
			and no_tablas = 'tbbloque'
		)) js_bloque
	);
	
	v_nu_compar := 0;
	
	-- PROCESO DE COMPARACION DE INSERT FW
	-- obtenemos todos los scripts de bloques que hayan cambiado
	raise notice 'insertando los cambios de bloques y updates a la tabla de paquete';
	
	for v_rs_bloque in
		select
			co_bloque,
			--co_bloque || ' - ' || f_no_bloque(co_bloque) as co_bloque,
			'delete from fwcontra.ttblopar where co_bloque = '|| co_bloque || ';' || chr(13) ||
			'delete from fwcontra.ttvenreg where co_bloque = '|| co_bloque || ';' as qr_deltra,
			fbscript_insert_migrar(co_bloque::varchar, v_co_blomax) qr_bloque,
			fbscript_update_botacc_migrar(co_bloque::varchar) qr_update
		from (select co_bloque::integer
			from regexp_split_to_table(
				v_ls_bloque, ','
			) co_bloque
			order by 1
		)tx
		order by co_bloque
	loop
		v_nu_compar := v_nu_compar + 1;
		
		v_qr_deltra := v_qr_deltra || v_rs_bloque.qr_deltra;
		v_qr_bloque := v_qr_bloque || v_rs_bloque.qr_bloque;
		v_qr_update := v_qr_update || v_rs_bloque.qr_update;
		
		select
			''::varchar as va_datcon,
			('--' || v_rs_bloque.co_bloque || chr(10) || v_rs_bloque.qr_deltra)::varchar as va_datori,
			v_rs_bloque.qr_update as va_datdes,
			v_rs_bloque.qr_bloque as va_sintax
		into v_rs_proces;
		
		insert into becombas.tbpaquet (id_compar, nu_compar, no_objeto, qr_objeto, qr_deltra, ti_paquet)
		values (139, v_nu_compar, 'Bloque ' || v_rs_bloque.co_bloque, v_rs_bloque.qr_bloque, v_qr_deltra, 2);
		
		v_ls_compar := (
			select string_agg(id_compar::varchar, ',')
			from becombas.tbpaquet
			where fe_paspaq is null
			and no_objeto = 'Bloque ' || v_rs_bloque.co_bloque
			and replace(replace(qr_objeto, chr(9), ''), ' ', '') <> replace(replace(v_rs_bloque.qr_bloque, chr(9), ''), ' ', '')
			and ti_paquet = 2
		);
		
		if v_ls_compar is not null then
			v_rs_proces.va_datcon = ('** CONFLICT ** id_compar: ' || v_ls_compar)::varchar; 
		end if;
		
		v_nu_compar := v_nu_compar + 1;
		
		insert into becombas.tbpaquet (id_compar, nu_compar, no_objeto, qr_objeto, ti_paquet)
		values (139, v_nu_compar, 'Bloque ' || v_rs_bloque.co_bloque, v_rs_bloque.qr_update, 3);
		
		v_ls_compar := (
			select string_agg(id_compar::varchar, ',')
			from becombas.tbpaquet
			where fe_paspaq is null
			and no_objeto = 'Bloque ' || v_rs_bloque.co_bloque
			--and qr_objeto <> v_rs_bloque.qr_update
			and replace(replace(qr_objeto, chr(9), ''), ' ', '') <> replace(replace(v_rs_bloque.qr_update, chr(9), ''), ' ', '')
			and id_compar <> 139
			and ti_paquet = 3
		);
		
		if v_ls_compar is not null then
			v_rs_proces.va_datcon = ('** CONFLICT ** id_compar: ' || v_ls_compar)::varchar; 
		end if;
		
		return next v_rs_proces;
	end loop;
	
	-- PROCESO DE COMPARACION DE UPDATE FW
	raise notice 'insertando los cambios de bloques y updates a la tabla de paquete';
	for v_rs_proces in
		select ''::varchar as va_datcon, * from becombas.f_comparar_db_sintaxis_v4(139, 3)
	loop
		v_nu_compar := v_nu_compar + 1;
		
		insert into becombas.tbpaquet (id_compar, nu_compar, no_objeto, qr_objeto, ti_paquet)
		values (139, v_nu_compar, v_rs_proces.va_datori, v_rs_proces.va_sintax, 3);
		
		v_ls_compar := (
			select string_agg(id_compar::varchar, ',')
			from becombas.tbpaquet
			where fe_paspaq is null
			and no_objeto = v_rs_proces.va_datori
			and qr_objeto <> v_rs_proces.va_sintax
			and id_compar <> 139
			and ti_paquet = 3
		);
		
		if v_ls_compar is not null then
			v_rs_proces.va_datcon = ('** CONFLICT ** id_compar: ' || v_ls_compar)::varchar; 
		end if;
		
		return next v_rs_proces;
	end loop;
	
	-- PROCESO DE COMPARACION DE INSERT FW
	raise notice ' INSERT WF los cambios de bloques: becombas.f_comparar_db_sintaxis_v4(139, 4)';
	for v_rs_proces in
		select ''::varchar as va_datcon, * from becombas.f_comparar_db_sintaxis_v4(139, 4)
	loop
		v_nu_compar := v_nu_compar + 1;
		
		insert into becombas.tbpaquet (id_compar, nu_compar, no_objeto, qr_objeto, ti_paquet)
		values (139, v_nu_compar, v_rs_proces.va_datori, v_rs_proces.va_sintax, 4);
		
		v_ls_compar := (
			select string_agg(id_compar::varchar, ',')
			from becombas.tbpaquet
			where fe_paspaq is null
			and no_objeto = v_rs_proces.va_datori
			and qr_objeto <> v_rs_proces.va_sintax
			and id_compar <> 139
			and ti_paquet = 4
		);
		
		if v_ls_compar is not null then
			v_rs_proces.va_datcon = ('** CONFLICT ** id_compar: ' || v_ls_compar)::varchar; 
		end if;
		
		return next v_rs_proces;
	end loop;
	
	-- PROCESO DE COMPARACION DE update wf tcraiexc y tcregist
	raise notice 'UPDATE WF tcraiexc y tcregist: from becombas.f_comparar_db_sintaxis_v4(139, 5)';
	for v_rs_proces in
		select ''::varchar as va_datcon, * from becombas.f_comparar_db_sintaxis_v4(139, 5)
	loop
		v_nu_compar := v_nu_compar + 1;
		
		insert into becombas.tbpaquet (id_compar, nu_compar, no_objeto, qr_objeto, ti_paquet)
		values (139, v_nu_compar, v_rs_proces.va_datori, v_rs_proces.va_sintax, 5);
		
		v_ls_compar := (
			select string_agg(id_compar::varchar, ',')
			from becombas.tbpaquet
			where fe_paspaq is null
			and no_objeto = v_rs_proces.va_datori
			and qr_objeto <> v_rs_proces.va_sintax
			and id_compar <> 139
			and ti_paquet = 5
		);
		
		if v_ls_compar is not null then
			v_rs_proces.va_datcon = ('** CONFLICT ** id_compar: ' || v_ls_compar)::varchar; 
		end if;
		
		return next v_rs_proces;
	end loop;
	
	-- PROCESO DE FUNCIONES
	raise notice 'Proceso de comparacion de FUNCIONES: becombas.f_comparar_db_sintaxis_v4(139, 6)';
	for v_rs_proces in
		select ''::varchar as va_datcon, * from becombas.f_comparar_db_sintaxis_v4(139, 6)
	loop
		v_nu_compar := v_nu_compar + 1;
		
		--v_rs_proces.va_sintax := becombas.f_variables_limpiar(v_rs_proces.va_sintax);
		
		insert into becombas.tbpaquet (id_compar, nu_compar, no_objeto, qr_objeto, ti_paquet)
		values (139, v_nu_compar, v_rs_proces.va_datori, v_rs_proces.va_sintax, 6);
		
		v_ls_compar := (
			select string_agg(id_compar::varchar, ',')
			from becombas.tbpaquet
			where fe_paspaq is null
			and no_objeto = v_rs_proces.va_datori
			and qr_objeto <> v_rs_proces.va_sintax
			and id_compar <> 139
			and ti_paquet = 6
		);
		
		if v_ls_compar is not null then
			v_rs_proces.va_datcon = ('** CONFLICT ** id_compar: ' || v_ls_compar)::varchar; 
		end if;
		
		return next v_rs_proces;
	end loop;
	
	return;
end;
$$;


ALTER FUNCTION becombas.f_push_proyecto() OWNER TO aguzman;

--
-- Name: f_ti_column(integer, integer, character varying); Type: FUNCTION; Schema: becombas; Owner: aguzman
--

CREATE FUNCTION becombas.f_ti_column(p_id_compar integer, p_ti_basdat integer, p_no_column character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
begin
	if p_ti_basdat = 1 then
		return (
			select ti_column
			from becombas.tbtabori
			where id_compar = p_id_compar
			and no_esquem = split_part(p_no_column, '.', 1)
			and no_tablas = split_part(p_no_column, '.', 2)
			and no_column = split_part(p_no_column, '.', 3)
		);
	elseif p_ti_basdat = 2 then
		return (
			select ti_column
			from becombas.tbtabdes
			where id_compar = p_id_compar
			and no_esquem = split_part(p_no_column, '.', 1)
			and no_tablas = split_part(p_no_column, '.', 2)
			and no_column = split_part(p_no_column, '.', 3)
		);
	else
		return null;
	end if;
	
	return null;
end;
$$;


ALTER FUNCTION becombas.f_ti_column(p_id_compar integer, p_ti_basdat integer, p_no_column character varying) OWNER TO aguzman;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: tbcompar; Type: TABLE; Schema: becombas; Owner: aguzman
--

CREATE TABLE becombas.tbcompar (
    id_compar integer NOT NULL,
    de_compar character varying NOT NULL,
    fe_compar timestamp without time zone DEFAULT now() NOT NULL,
    no_basori character varying,
    de_basori character varying,
    no_basdes character varying,
    de_basdes character varying,
    il_estado boolean DEFAULT true NOT NULL
);


ALTER TABLE becombas.tbcompar OWNER TO aguzman;

--
-- Name: qbcompar; Type: SEQUENCE; Schema: becombas; Owner: aguzman
--

CREATE SEQUENCE becombas.qbcompar
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE becombas.qbcompar OWNER TO aguzman;

--
-- Name: qbcompar; Type: SEQUENCE OWNED BY; Schema: becombas; Owner: aguzman
--

ALTER SEQUENCE becombas.qbcompar OWNED BY becombas.tbcompar.id_compar;


--
-- Name: tbcatdes; Type: TABLE; Schema: becombas; Owner: aguzman
--

CREATE TABLE becombas.tbcatdes (
    id_compar integer NOT NULL,
    nu_compar integer NOT NULL,
    no_esquem character varying NOT NULL,
    no_tablas character varying NOT NULL,
    js_conten character varying
);


ALTER TABLE becombas.tbcatdes OWNER TO aguzman;

--
-- Name: TABLE tbcatdes; Type: COMMENT; Schema: becombas; Owner: aguzman
--

COMMENT ON TABLE becombas.tbcatdes IS 'Tabla base de los objetos de base de datos de destino';


--
-- Name: tbcatori; Type: TABLE; Schema: becombas; Owner: aguzman
--

CREATE TABLE becombas.tbcatori (
    id_compar integer NOT NULL,
    nu_compar integer NOT NULL,
    no_esquem character varying NOT NULL,
    no_tablas character varying NOT NULL,
    js_conten character varying
);


ALTER TABLE becombas.tbcatori OWNER TO aguzman;

--
-- Name: TABLE tbcatori; Type: COMMENT; Schema: becombas; Owner: aguzman
--

COMMENT ON TABLE becombas.tbcatori IS 'Tabla base de los objetos de base de datos de origen';


--
-- Name: tbcondes; Type: TABLE; Schema: becombas; Owner: aguzman
--

CREATE TABLE becombas.tbcondes (
    id_compar integer NOT NULL,
    nu_compar integer NOT NULL,
    ti_constr character(1) NOT NULL,
    no_esquem character varying NOT NULL,
    no_tablas character varying NOT NULL,
    no_constr character varying NOT NULL,
    de_coment character varying NOT NULL,
    df_constr character varying
);


ALTER TABLE becombas.tbcondes OWNER TO aguzman;

--
-- Name: tbconori; Type: TABLE; Schema: becombas; Owner: aguzman
--

CREATE TABLE becombas.tbconori (
    id_compar integer NOT NULL,
    nu_compar integer NOT NULL,
    ti_constr character(1) NOT NULL,
    no_esquem character varying NOT NULL,
    no_tablas character varying NOT NULL,
    no_constr character varying NOT NULL,
    de_coment character varying NOT NULL,
    df_constr character varying
);


ALTER TABLE becombas.tbconori OWNER TO aguzman;

--
-- Name: tbfundes; Type: TABLE; Schema: becombas; Owner: aguzman
--

CREATE TABLE becombas.tbfundes (
    id_compar integer NOT NULL,
    nu_compar integer NOT NULL,
    no_esquem character varying NOT NULL,
    no_funcio character varying NOT NULL,
    de_coment character varying NOT NULL,
    ca_parfun smallint NOT NULL,
    ls_parfun character varying NOT NULL,
    df_funcio character varying NOT NULL,
    ls_partyp character varying
);


ALTER TABLE becombas.tbfundes OWNER TO aguzman;

--
-- Name: tbfunori; Type: TABLE; Schema: becombas; Owner: aguzman
--

CREATE TABLE becombas.tbfunori (
    id_compar integer NOT NULL,
    nu_compar integer NOT NULL,
    no_esquem character varying NOT NULL,
    no_funcio character varying NOT NULL,
    de_coment character varying NOT NULL,
    ca_parfun smallint NOT NULL,
    ls_parfun character varying NOT NULL,
    df_funcio character varying NOT NULL,
    ls_partyp character varying
);


ALTER TABLE becombas.tbfunori OWNER TO aguzman;

--
-- Name: tbobjdes; Type: TABLE; Schema: becombas; Owner: aguzman
--

CREATE TABLE becombas.tbobjdes (
    id_compar integer NOT NULL,
    nu_compar integer NOT NULL,
    ti_objeto character(1) NOT NULL,
    no_esquem character varying NOT NULL,
    no_objeto character varying NOT NULL,
    de_coment character varying NOT NULL,
    js_objeto character varying
);


ALTER TABLE becombas.tbobjdes OWNER TO aguzman;

--
-- Name: tbobjori; Type: TABLE; Schema: becombas; Owner: aguzman
--

CREATE TABLE becombas.tbobjori (
    id_compar integer NOT NULL,
    nu_compar integer NOT NULL,
    ti_objeto character(1) NOT NULL,
    no_esquem character varying NOT NULL,
    no_objeto character varying NOT NULL,
    de_coment character varying NOT NULL,
    js_objeto character varying
);


ALTER TABLE becombas.tbobjori OWNER TO aguzman;

--
-- Name: tbpaquet; Type: TABLE; Schema: becombas; Owner: aguzman
--

CREATE TABLE becombas.tbpaquet (
    id_compar integer NOT NULL,
    nu_compar integer NOT NULL,
    no_objeto character varying NOT NULL,
    fe_regist timestamp(0) with time zone DEFAULT now() NOT NULL,
    qr_objeto character varying NOT NULL,
    qr_deltra character varying,
    ti_paquet smallint,
    fe_paspaq timestamp(0) with time zone
);


ALTER TABLE becombas.tbpaquet OWNER TO aguzman;

--
-- Name: TABLE tbpaquet; Type: COMMENT; Schema: becombas; Owner: aguzman
--

COMMENT ON TABLE becombas.tbpaquet IS 'Tabla base de los objetos del paquete de pase';


--
-- Name: COLUMN tbpaquet.id_compar; Type: COMMENT; Schema: becombas; Owner: aguzman
--

COMMENT ON COLUMN becombas.tbpaquet.id_compar IS 'Identificador de comparacion de base de datos';


--
-- Name: COLUMN tbpaquet.nu_compar; Type: COMMENT; Schema: becombas; Owner: aguzman
--

COMMENT ON COLUMN becombas.tbpaquet.nu_compar IS 'Número secuencial de los objetos del paquete';


--
-- Name: COLUMN tbpaquet.no_objeto; Type: COMMENT; Schema: becombas; Owner: aguzman
--

COMMENT ON COLUMN becombas.tbpaquet.no_objeto IS 'Nombre del objeto del paquete';


--
-- Name: COLUMN tbpaquet.fe_regist; Type: COMMENT; Schema: becombas; Owner: aguzman
--

COMMENT ON COLUMN becombas.tbpaquet.fe_regist IS 'Fecha de registro';


--
-- Name: COLUMN tbpaquet.qr_objeto; Type: COMMENT; Schema: becombas; Owner: aguzman
--

COMMENT ON COLUMN becombas.tbpaquet.qr_objeto IS 'Sintaxis del objeto del paquete';


--
-- Name: COLUMN tbpaquet.qr_deltra; Type: COMMENT; Schema: becombas; Owner: aguzman
--

COMMENT ON COLUMN becombas.tbpaquet.qr_deltra IS 'Sintaxis de la eliminación de transacciones del objeto';


--
-- Name: COLUMN tbpaquet.ti_paquet; Type: COMMENT; Schema: becombas; Owner: aguzman
--

COMMENT ON COLUMN becombas.tbpaquet.ti_paquet IS 'Tipo de paquete: 1 Estructura, 2 Insert FW, 3 Update Fw, 4 Insert WF, 5 Update WF, 6 Funciones';


--
-- Name: tbtabdes; Type: TABLE; Schema: becombas; Owner: aguzman
--

CREATE TABLE becombas.tbtabdes (
    id_compar integer NOT NULL,
    nu_compar integer NOT NULL,
    no_esquem character varying NOT NULL,
    no_tablas character varying NOT NULL,
    nu_ordcol smallint NOT NULL,
    no_column character varying,
    ti_column character varying,
    va_defaul character varying,
    va_notnul character varying,
    va_prikey character varying,
    va_forkey character varying,
    de_coment character varying
);


ALTER TABLE becombas.tbtabdes OWNER TO aguzman;

--
-- Name: tbtabori; Type: TABLE; Schema: becombas; Owner: aguzman
--

CREATE TABLE becombas.tbtabori (
    id_compar integer NOT NULL,
    nu_compar integer NOT NULL,
    no_esquem character varying NOT NULL,
    no_tablas character varying NOT NULL,
    nu_ordcol smallint NOT NULL,
    no_column character varying,
    ti_column character varying,
    va_defaul character varying,
    va_notnul character varying,
    va_prikey character varying,
    va_forkey character varying,
    de_coment character varying
);


ALTER TABLE becombas.tbtabori OWNER TO aguzman;

--
-- Name: tbcompar id_compar; Type: DEFAULT; Schema: becombas; Owner: aguzman
--

ALTER TABLE ONLY becombas.tbcompar ALTER COLUMN id_compar SET DEFAULT nextval('becombas.qbcompar'::regclass);


--
-- Name: tbtabori tbbasrem_pkey; Type: CONSTRAINT; Schema: becombas; Owner: aguzman
--

ALTER TABLE ONLY becombas.tbtabori
    ADD CONSTRAINT tbbasrem_pkey PRIMARY KEY (id_compar, nu_compar);


--
-- Name: tbcatdes tbcatdes_pkey; Type: CONSTRAINT; Schema: becombas; Owner: aguzman
--

ALTER TABLE ONLY becombas.tbcatdes
    ADD CONSTRAINT tbcatdes_pkey PRIMARY KEY (id_compar, nu_compar);


--
-- Name: tbcatori tbcatori_pkey; Type: CONSTRAINT; Schema: becombas; Owner: aguzman
--

ALTER TABLE ONLY becombas.tbcatori
    ADD CONSTRAINT tbcatori_pkey PRIMARY KEY (id_compar, nu_compar);


--
-- Name: tbcompar tbcompar_pkey; Type: CONSTRAINT; Schema: becombas; Owner: aguzman
--

ALTER TABLE ONLY becombas.tbcompar
    ADD CONSTRAINT tbcompar_pkey PRIMARY KEY (id_compar);


--
-- Name: tbcondes tbcondes_pkey; Type: CONSTRAINT; Schema: becombas; Owner: aguzman
--

ALTER TABLE ONLY becombas.tbcondes
    ADD CONSTRAINT tbcondes_pkey PRIMARY KEY (id_compar, nu_compar);


--
-- Name: tbconori tbconori_pkey; Type: CONSTRAINT; Schema: becombas; Owner: aguzman
--

ALTER TABLE ONLY becombas.tbconori
    ADD CONSTRAINT tbconori_pkey PRIMARY KEY (id_compar, nu_compar);


--
-- Name: tbfundes tbfundes_pkey; Type: CONSTRAINT; Schema: becombas; Owner: aguzman
--

ALTER TABLE ONLY becombas.tbfundes
    ADD CONSTRAINT tbfundes_pkey PRIMARY KEY (id_compar, nu_compar);


--
-- Name: tbfunori tbfunori_pkey; Type: CONSTRAINT; Schema: becombas; Owner: aguzman
--

ALTER TABLE ONLY becombas.tbfunori
    ADD CONSTRAINT tbfunori_pkey PRIMARY KEY (id_compar, nu_compar);


--
-- Name: tbobjdes tbobjdes_pkey; Type: CONSTRAINT; Schema: becombas; Owner: aguzman
--

ALTER TABLE ONLY becombas.tbobjdes
    ADD CONSTRAINT tbobjdes_pkey PRIMARY KEY (id_compar, nu_compar);


--
-- Name: tbobjori tbobjori_pkey; Type: CONSTRAINT; Schema: becombas; Owner: aguzman
--

ALTER TABLE ONLY becombas.tbobjori
    ADD CONSTRAINT tbobjori_pkey PRIMARY KEY (id_compar, nu_compar);


--
-- Name: tbpaquet tbpaquet_pkey; Type: CONSTRAINT; Schema: becombas; Owner: aguzman
--

ALTER TABLE ONLY becombas.tbpaquet
    ADD CONSTRAINT tbpaquet_pkey PRIMARY KEY (id_compar, nu_compar);


--
-- Name: tbtabdes tbtabdes_pkey; Type: CONSTRAINT; Schema: becombas; Owner: aguzman
--

ALTER TABLE ONLY becombas.tbtabdes
    ADD CONSTRAINT tbtabdes_pkey PRIMARY KEY (id_compar, nu_compar);


--
-- Name: tbcatdes tbcatdes_id_compar_fkey; Type: FK CONSTRAINT; Schema: becombas; Owner: aguzman
--

ALTER TABLE ONLY becombas.tbcatdes
    ADD CONSTRAINT tbcatdes_id_compar_fkey FOREIGN KEY (id_compar) REFERENCES becombas.tbcompar(id_compar);


--
-- PostgreSQL database dump complete
--

