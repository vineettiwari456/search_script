append_sms_sent_by_query_body = {
    "script": {
        "lang": "painless",
        "source": "ctx._source.sms_sent_by.add(params.subuid)"
    }
}

append_viewed_by_query_body = {
    "script": {
        "lang": "painless",
        "source": "ctx._source.viewed_by.add(params.subuid)"
    }
}

append_downloaded_by_query_body = {
    "script": {
        "lang": "painless",
        "source": "ctx._source.downloaded_by.add(params.subuid)"
    }
}

append_emailed_by_query_body = {
    "script": {
        "lang": "painless",
        "source": "ctx._source.emailed_by.add(params.subuid)"
    }
}

append_invitation_sent_by_query_body = {
    "script": {
        "lang": "painless",
        "source": "ctx._source.invitation_sent_by.add(params.subuid)"
    }
}

append_comments_query_body = {
    "script": {
        "lang": "painless",
        "source": "for (profile in ctx._source.profiles){if (profile.kiwi_profile_id==params.kiwi_profile_id){"
                  "profile.comments.add(params.comment_ob)}} "
    }
}

append_comments_new_query_body = {
    "script": {
        "lang": "painless",
        "source": "for (profile in ctx._source.profiles){if(profile.profile_id==params.profile_id)"
                  "{profile.comments.add(params.comment_ob)}}"
    }
}

append_comments_old_query_body = {
    "script": {
        "lang": "painless",
        "source": "for (profile in ctx._source.profiles){if(profile.kiwi_profile_id==params.profile_id)" 
                  "{profile.comments.add(params.comment_ob)}}"
    }
}

STORED_SCRIPTS_QUERIES = {
    "append_sms_sent_by": append_sms_sent_by_query_body,
    "append_viewed_by": append_viewed_by_query_body,
    "append_emailed_by": append_emailed_by_query_body,
    "append_downloaded_by": append_downloaded_by_query_body,
    "append_invitation_sent_by": append_invitation_sent_by_query_body,
    "append_comments": append_comments_query_body,
    "append_comments_new": append_comments_new_query_body,
    "append_comments_old": append_comments_old_query_body
}

STORED_SCRIPTS_END_POINT = "/_cluster/state/metadata?pretty&filter_path=**.stored_scripts"
