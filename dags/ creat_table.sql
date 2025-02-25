-- DROP TABLE public.dsllm_deduped_1;

CREATE TABLE public.dsllm_parsed (
	src_id uuid,
	create_date timestamp,
	update_date timestamp,
	src_type varchar(100),
	src_sub_type varchar(100),
	job_id varchar(200),
	raw text,
	markdown text,
	parsed text,
	del_yn bool,
	modified_date timestamp,
	src_create_date timestamp,
	src_modified_date timestamp,
	tag_list text[],
	wgt_param_list text[],
	add_info json
);

--DROP TABLE dsllm_deduped

CREATE TABLE public.dsllm_deduped (
	src_id uuid,
	create_date timestamp,
	update_date timestamp,
	src_type varchar(100),
	src_sub_type varchar(100),
	job_id varchar(200),
	raw text,
	del_yn bool,
	modified_date timestamp,
	src_create_date timestamp,
	src_modified_date timestamp,
	page_list text[],	
	wgt_param_list text[],
	add_info json
);

-- DROP TABLE public.dsllm_parsed_img

create table public.dsllm_parsed_img (
    src_id uuid,
    img_id uuid,
    src_img_url text,
    create_time timestamp
)


create table public.dsllm_job_hist (
    job_id uuid,
    job_type varchar(100),
    job_end_point varchar(100),
    file_path_list text[],
    src_type varchar(100),
    src_sub_type varchar(100),
    after_job_id uuid,
    prev_job_id uuid,
    data bytea,
    err_msg text,
    create_time timestamp
)
