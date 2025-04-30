select
    spec.label as specimen_label,
    spec.identifier as specimen_id,
    form.de_a_15 as form_nc_details,
    form.de_a_16 as form_nc_reason,
    form.de_a_23 as form_sop,
    form.de_a_37 as form_storage_container,
    form.de_a_39 as form_technitian,
    custom_field.identifier as custom_field_record_id,
    custom_field_form_context_id,
    custom_field.de_a_11 as custom_field_nc_details,
    non_conf_reason_value,
    sop_value,
    custom_field.de_a_4 as custom_field_storage_tube,
    custom_field.de_a_5 as custom_field_request_id
from
    catissue_specimen spec
    join (
        select
            rec.object_id,
            form.identifier,
            form.de_a_15,
            form.de_a_16,
            form.de_a_23,
            form.de_a_37,
            form.de_a_39
        from
            catissue_form_record_entry rec
            join catissue_form_context ctxt on rec.form_ctxt_id = ctxt.identifier
            join de_e_11301 form on form.identifier = rec.record_id
        where
            ctxt.container_id = 376
    ) form on form.object_id = spec.identifier
    left join (
        select
            rec.object_id,
            rec.form_ctxt_id as custom_field_form_context_id,
            custom_field.identifier,
            custom_field.de_a_11,
            custom_field.de_a_4,
            custom_field.de_a_5,
            non_conf_reason.value as non_conf_reason_value,
            sop.value as sop_value
        from
            catissue_form_record_entry rec
            join catissue_form_context ctxt on rec.form_ctxt_id = ctxt.identifier
            join de_e_11154 custom_field on custom_field.identifier = rec.record_id
            left join de_e_11155 non_conf_reason on non_conf_reason.record_id = custom_field.identifier
            left join de_e_11158 sop on sop.record_id = custom_field.identifier
        where
            ctxt.container_id = 226
    ) custom_field on custom_field.object_id = spec.identifier
where
    spec.activity_status != 'Disabled'