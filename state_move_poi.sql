create or replace function layer.state_move_poi(
	temp_id_table character varying,
	temp_schema_name character varying,
	from_table character varying,
	from_schema_name character varying,
	to_table character varying,
	to_table_schema character varying,
	from_state_code character varying,
	to_state_code character varying )
	
	RETURNS integer
	LANGUAGE 'plpgsql'
	
AS $body$

DECLARE 
DECLARE returnstatus varchar;
DECLARE count integer ;
DECLARE r record;

DECLARE colstring text;
DECLARE colstring_el text;
DECLARE colstring_ens text;
DECLARE colstring_es text;

DECLARE state_cd text ;
DECLARE SQLQuery text;
DECLARE SQLQuery1 text;
DECLARE sqlq text;

DECLARE table_count integer;
DECLARE from_count integer;
DECLARE  integer;
DECLARE to_count integer;
DECLARE stat_code text;
DECLARE stt_id integer;
DECLARE stt_code varchar;

DECLARE from_poi_tab_name varchar;
DECLARE from_el_tab_name varchar;
DECLARE from_non_shift_tab_name varchar;
DECLARE from_shift_tab_name varchar;

DECLARE to_poi_tab_name varchar;
DECLARE to_el_tab_name varchar;
DECLARE to_non_shift_tab_name varchar;
DECLARE to_shift_tab_name varchar;

DECLARE f1 text ;
DECLARE f2 text;
DECLARE t1 text;
DECLARE t2 text;

DECLARE error_tab_name varchar;

begin
	returnstatus =-1;
	error_tab_name ='layer.error';
	
if from_table ='POI' and to_table = 'POI' then


	stat_code=''||from_state_code||'';
	raise warning 'state :%',stat_code;
	
	stat_cod=''||to_state_code||'';
	raise warning 'state :%',stat_cod;
	
	SQLQuery = 'select count(table_name) from information_schema.tables where upper(table_name) like '''||upper(stat_code)||'_POI''
	and table_schema ='''||from_schema_name||'''';
	execute SQLQuery into from_count;
	raise info 'POI_count %',from_count;
	
	
	SQLQuery = 'select count(table_name) from information_schema.tables where upper(table_name) like '''||upper(stat_cod)||'_POI''
	and table_schema ='''||to_schema_name||'''';
	execute SQLQuery into to_count;
	raise info 'POI_count %',to_count;
	
	if from_count =1 and to_count = 1 then
	
			from_poi_tab_name = stat_code||'_POI';
			from_el_tab_name  = stat_code||'_POI_EDGELINE';
			from_non_shift_tab_name  = stat_code||'_POI_ENTRYNONSHIFT';
			from_shift_tab_name  = stat_code||'_POI_ENTRYSHIFT';

			to_poi_tab_name  = stat_cod||'_POI';
			to_el_tab_name  = stat_cod||'_POI_EDGELINE';
			to_non_shift_tab_name  = stat_cod||'_POI_ENTRYNONSHIFT';
			to_shift_tab_name  = stat_cod||'_POI_ENTRYSHIFT';
			
			
		SQLQuery = 'select count * from '||temp_schema_name||'."'||temp_id_table||'"';
		execute SQLQuery into table_count;
		raise info 'POI_count %',table_count;
			
	
	
		SQLQuery = format('select count(table_name) from information_schema.tables where upper(table_name) like ''%1$s_POI'' or  upper(table_name) like ''%1$_POI_EDGELINE'' or
		upper(table_name) like ''%1$_POI_ENTRYNONSHIFT'' or  upper(table_name) like ''%1$_POI_ENTRYSHIFT''',stat_code,from_schema_name);
		execute SQLQuery into count;
		raise info 'POI_count %',count;
	
		SQLQuery = format('select count(table_name) from information_schema.tables where upper(table_name) like ''%1$s_POI'' or  upper(table_name) like ''%1$_POI_EDGELINE'' or
		upper(table_name) like ''%1$_POI_ENTRYNONSHIFT'' or  upper(table_name) like ''%1$_POI_ENTRYSHIFT''',stat_cod,to_schema_name);
		execute SQLQuery into tpd_count;
		raise info 'POI_count %',tpd_count;
		 
		--get col string without MI_PRINX
		
		colstring = array_to_string(array(select case when column_name::text=upper(column_name::text)then '"'||column_name::text||'"' else column_name::text
		end from information_schema.columns where table_name =''||from_poi_tab_name||'' and table_schema=''||from_state_code||'' and column_name not in ('MI_PRINX'))
		,',');
		
		raise info 'colstring POI:%',colstring;
		
		colstring_el = array_to_string(array(select case when column_name::text=upper(column_name::text)then '"'||column_name::text||'"' else column_name::text
		end from information_schema.columns where table_name =''||from_el_tab_name||'' and table_schema=''||from_state_code||'' and column_name not in ('MI_PRINX'))
		,',');
		
		raise info 'colstring POI:%',colstring_el;
		
		
		colstring_ens = array_to_string(array(select case when column_name::text=upper(column_name::text)then '"'||column_name::text||'"' else column_name::text
		end from information_schema.columns where table_name =''||from_non_shift_tab_name||'' and table_schema=''||from_state_code||'' and column_name not in ('MI_PRINX'))
		,',');
		
		raise info 'colstring POI:%',colstring_ens;
		
		
		colstring_es = array_to_string(array(select case when column_name::text=upper(column_name::text)then '"'||column_name::text||'"' else column_name::text
		end from information_schema.columns where table_name =''||from_shift_tab_name||'' and table_schema=''||from_state_code||'' and column_name not in ('MI_PRINX'))
		,',');
		
		raise info 'colstring POI:%',colstring_es;



		--insert into from_state to to_state
		
		SQLQuery = 'insert into '||to_schema_name||'."'||to_poi_tab_name||'" ('||colstring||') ( select '||colstring||' from  
		'||from_schema_name||'."'||from_poi_tab_name||'"  where "ID" in (select "ID" from '||temp_schema_name||'."'||temp_id_table||'"))';
		
		raise info '<----->data is insert into table ------>:%',SQLQuery;
		execute SQLQuery;
		
		
		SQLQuery = 'insert into '||to_schema_name||'."'||to_el_tab_name||'" ('||colstring||') ( select '||colstring||' from  
		'||from_schema_name||'."'||from_el_tab_name||'"  where "ID" in (select "ID" from '||temp_schema_name||'."'||temp_id_table||'"))';
		
		raise info '<----->data is insert into table ------>:%',SQLQuery;
		execute SQLQuery;
		
		
		SQLQuery = 'insert into '||to_schema_name||'."'||to_non_shift_tab_name||'" ('||colstring||') ( select '||colstring||' from  
		'||from_schema_name||'."'||from_non_shift_tab_name||'"  where "ID" in (select "ID" from '||temp_schema_name||'."'||temp_id_table||'"))';
		
		raise info '<----->data is insert into table ------>:%',SQLQuery;
		execute SQLQuery;
		
		
		SQLQuery = 'insert into '||to_schema_name||'."'||to_shift_tab_name||'" ('||colstring||') ( select '||colstring||' from  
		'||from_schema_name||'."'||from_shift_tab_name||'"  where "ID" in (select "ID" from '||temp_schema_name||'."'||temp_id_table||'"))';
		
		raise info '<----->data is insert into table ------>:%',SQLQuery;
		execute SQLQuery;
		
		
		--- delete from from_table
		
		SQLQuery = 'delete from '||from_schema_name||'."'||from_poi_tab_name||'"  
		where "ID" in (select "ID" from '||temp_schema_name||'."'||temp_id_table||'"))';
		
		raise info '<----->data is delete into table ------>:%',SQLQuery;
		execute SQLQuery;
		
		
		SQLQuery = 'delete from  '||from_schema_name||'."'||from_el_tab_name||'"  
		where "ID" in (select "ID" from '||temp_schema_name||'."'||temp_id_table||'"))';
		
		raise info '<----->data is delete into table ------>:%',SQLQuery;
		execute SQLQuery;
		
		
		SQLQuery = 'delete from '||from_schema_name||'."'||from_non_shift_tab_name||'" 
		where "ID" in (select "ID" from '||temp_schema_name||'."'||temp_id_table||'"))';
		
		raise info '<----->data is delete into table ------>:%',SQLQuery;
		execute SQLQuery;
		
		
		SQLQuery = 'delete from '||from_schema_name||'."'||from_shift_tab_name||'" 
		where "ID" in (select "ID" from '||temp_schema_name||'."'||temp_id_table||'"))';
		
		raise info '<----->data is delete into table ------>:%',SQLQuery;
		execute SQLQuery;
		
		elseif from_count = 0 then
			
			SQLQuery = 'select count(table_name) from information_schema.tables where upper(table_name) like '''||upper(stat_code)||'_P%_POI''
			and table_schema ='''||from_schema_name||'''';
			execute SQLQuery into count;
			raise info 'POI_count %',count;
			
			if count > 1 then
			SQLQuery = 'select count(table_name) from information_schema.tables where upper(table_name) like '''||upper(stat_code)||'_P%_POI''
			and table_schema ='''||from_schema_name||'''';
			
			for r_tab_name in execute SQLQuery
			loop
				poi_tab_name = r_tab_name."table_name";
				raise info 'process for table:%',poi_tab_name;
				
						from_poi_tab_name = ''||poi_tab_name||'';
						from_el_tab_name  = stat_code||'_POI_EDGELINE';
						from_non_shift_tab_name  = stat_code||'_POI_ENTRYNONSHIFT';
						from_shift_tab_name  = stat_code||'_POI_ENTRYSHIFT';

						to_poi_tab_name  = ''||poi_tab_name||'';
						to_el_tab_name  = stat_cod||'_POI_EDGELINE';
						to_non_shift_tab_name  = stat_cod||'_POI_ENTRYNONSHIFT';
						to_shift_tab_name  = stat_cod||'_POI_ENTRYSHIFT';
				
				
				SQLQuery = 'select count * from '||temp_schema_name||'."'||temp_id_table||'"';
				execute SQLQuery into table_count;
				raise info 'POI_count %',table_count;
			
				SQLQuery = format('select count(table_name) from information_schema.tables where upper(table_name) like ''%1$s_POI'' or  upper(table_name) like ''%1$_POI_EDGELINE'' or
				upper(table_name) like ''%1$_POI_ENTRYNONSHIFT'' or  upper(table_name) like ''%1$_POI_ENTRYSHIFT''',stat_code,from_schema_name);
				execute SQLQuery into count;
				raise info 'POI_count %',count;
			
				SQLQuery = format('select count(table_name) from information_schema.tables where upper(table_name) like ''%1$s_POI'' or  upper(table_name) like ''%1$_POI_EDGELINE'' or
				upper(table_name) like ''%1$_POI_ENTRYNONSHIFT'' or  upper(table_name) like ''%1$_POI_ENTRYSHIFT''',stat_cod,to_schema_name);
				execute SQLQuery into tpd_count;
				raise info 'tpd_count %',tpd_count;
				
				if (count >0 and tpd_count >0)then
				
					--get col string without MI_PRINX
		
					colstring = array_to_string(array(select case when column_name::text=upper(column_name::text)then '"'||column_name::text||'"' else column_name::text
					end from information_schema.columns where table_name =''||from_poi_tab_name||'' and table_schema=''||from_state_code||'' and column_name not in ('MI_PRINX'))
					,',');
					
					raise info 'colstring POI:%',colstring;
					
					colstring_el = array_to_string(array(select case when column_name::text=upper(column_name::text)then '"'||column_name::text||'"' else column_name::text
					end from information_schema.columns where table_name =''||from_el_tab_name||'' and table_schema=''||from_state_code||'' and column_name not in ('MI_PRINX'))
					,',');
					
					raise info 'colstring POI:%',colstring_el;
					
					
					colstring_ens = array_to_string(array(select case when column_name::text=upper(column_name::text)then '"'||column_name::text||'"' else column_name::text
					end from information_schema.columns where table_name =''||from_non_shift_tab_name||'' and table_schema=''||from_state_code||'' and column_name not in ('MI_PRINX'))
					,',');
					
					raise info 'colstring POI:%',colstring_ens;
					
					
					colstring_es = array_to_string(array(select case when column_name::text=upper(column_name::text)then '"'||column_name::text||'"' else column_name::text
					end from information_schema.columns where table_name =''||from_shift_tab_name||'' and table_schema=''||from_state_code||'' and column_name not in ('MI_PRINX'))
					,',');
					
					raise info 'colstring POI:%',colstring_es;



					--insert into from_state to to_state
					
					SQLQuery = 'insert into '||to_schema_name||'."'||to_poi_tab_name||'" ('||colstring||') ( select '||colstring||' from  
					'||from_schema_name||'."'||from_poi_tab_name||'"  where "ID" in (select "ID" from '||temp_schema_name||'."'||temp_id_table||'"))';
					
					raise info '<----->data is insert into table ------>:%',SQLQuery;
					execute SQLQuery;
					
					
					SQLQuery = 'insert into '||to_schema_name||'."'||to_el_tab_name||'" ('||colstring||') ( select '||colstring||' from  
					'||from_schema_name||'."'||from_el_tab_name||'"  where "ID" in (select "ID" from '||temp_schema_name||'."'||temp_id_table||'"))';
					
					raise info '<----->data is insert into table ------>:%',SQLQuery;
					execute SQLQuery;
					
					
					SQLQuery = 'insert into '||to_schema_name||'."'||to_non_shift_tab_name||'" ('||colstring||') ( select '||colstring||' from  
					'||from_schema_name||'."'||from_non_shift_tab_name||'"  where "ID" in (select "ID" from '||temp_schema_name||'."'||temp_id_table||'"))';
					
					raise info '<----->data is insert into table ------>:%',SQLQuery;
					execute SQLQuery;
					
					
					SQLQuery = 'insert into '||to_schema_name||'."'||to_shift_tab_name||'" ('||colstring||') ( select '||colstring||' from  
					'||from_schema_name||'."'||from_shift_tab_name||'"  where "ID" in (select "ID" from '||temp_schema_name||'."'||temp_id_table||'"))';
					
					raise info '<----->data is insert into table ------>:%',SQLQuery;
					execute SQLQuery;
					
					
					--- delete from from_table
					
					SQLQuery = 'delete from '||from_schema_name||'."'||from_poi_tab_name||'"  
					where "ID" in (select "ID" from '||temp_schema_name||'."'||temp_id_table||'"))';
					
					raise info '<----->data is delete into table ------>:%',SQLQuery;
					execute SQLQuery;
					
					
					SQLQuery = 'delete from  '||from_schema_name||'."'||from_el_tab_name||'"  
					where "ID" in (select "ID" from '||temp_schema_name||'."'||temp_id_table||'"))';
					
					raise info '<----->data is delete into table ------>:%',SQLQuery;
					execute SQLQuery;
					
					
					SQLQuery = 'delete from '||from_schema_name||'."'||from_non_shift_tab_name||'" 
					where "ID" in (select "ID" from '||temp_schema_name||'."'||temp_id_table||'"))';
					
					raise info '<----->data is delete into table ------>:%',SQLQuery;
					execute SQLQuery;
					
					
					SQLQuery = 'delete from '||from_schema_name||'."'||from_shift_tab_name||'" 
					where "ID" in (select "ID" from '||temp_schema_name||'."'||temp_id_table||'"))';
					
					raise info '<----->data is delete into table ------>:%',SQLQuery;
					execute SQLQuery;
					
				end if;
			
			else 
				raise info '<------> from_table and to_table is different';
				return returnstatus;
		end if ;
		return 1;
exception
		when others then 
		get stacked diagnostics
		f1 = message_text,
		f2 = pg_exception_context;
		raise info 'error caught :%',f1;
		raise info 'error caught :%',f2;
		
		return returnstatus;
	end
	
	$BODY$;
		
				


