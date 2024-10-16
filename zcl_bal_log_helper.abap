CLASS zcl_bal_log_helper DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    TYPES:
      BEGIN OF ty_s_result,
        kostl TYPE kostl,
        prctr TYPE prctr,
      END OF ty_s_result .
    TYPES:
      ty_t_result TYPE HASHED TABLE OF ty_s_result
                  WITH UNIQUE KEY kostl prctr .
    TYPES:
      ty_r_kostl TYPE RANGE OF kostl .

    CLASS-METHODS bal_db_delete
      IMPORTING
        !it_log_header TYPE balhdr_t .
    CLASS-METHODS bal_db_save
      IMPORTING
        !im_handle TYPE balloghndl OPTIONAL .
    CLASS-METHODS bal_db_search
      IMPORTING
        !iv_subobject        TYPE balsubobj OPTIONAL
        !iv_object           TYPE balobj_d
      RETURNING
        VALUE(rt_log_header) TYPE balhdr_t .
    CLASS-METHODS bal_log_create
      IMPORTING
        !iv_extnumber   TYPE balnrext OPTIONAL
        !iv_subobject   TYPE balsubobj OPTIONAL
        !iv_object      TYPE balobj_d
      RETURNING
        VALUE(r_handle) TYPE balloghndl .
    CLASS-METHODS bal_log_hdr_read
      IMPORTING
        !iv_handle           TYPE balloghndl
      RETURNING
        VALUE(rs_statistics) TYPE bal_s_scnt .
    CLASS-METHODS bal_log_msg_add
      IMPORTING
        !iv_handle     TYPE balloghndl
        !iv_tabname    TYPE baltabname OPTIONAL
        !iv_sort_field TYPE char6 OPTIONAL
        !iv_field      TYPE text60 OPTIONAL
        !iv_field_text TYPE text60 OPTIONAL .
    CLASS-METHODS bal_log_refresh
      IMPORTING
        !iv_handle    TYPE balloghndl
      RETURNING
        VALUE(r_bool) TYPE abap_bool .
    CLASS-METHODS select_cost_centers
      IMPORTING
        !it_kostl        TYPE ty_r_kostl
      RETURNING
        VALUE(rt_result) TYPE ty_t_result .
    CLASS-METHODS bal_log_hdr_change
      IMPORTING
        !i_log_handle TYPE balloghndl
        !i_s_log      TYPE bal_s_log .
    CLASS-METHODS bal_db_read
      IMPORTING
        !i_log_handle TYPE balloghndl
      RETURNING
        VALUE(rs_log) TYPE bal_s_log .
    CLASS-METHODS bal_db_load
      IMPORTING
        !iv_handle TYPE balloghndl .
  PROTECTED SECTION.
  PRIVATE SECTION.

    CONSTANTS c_object TYPE balobj_d VALUE 'ZPS' ##NO_TEXT.
    CONSTANTS c_subobject TYPE balsubobj VALUE 'PTOOL' ##NO_TEXT.

ENDCLASS.

CLASS zcl_bal_log_helper IMPLEMENTATION.


  METHOD bal_db_delete.
    IF NOT it_log_header IS INITIAL.
      DATA(lt_log_handle) = VALUE bal_t_logh( FOR <fs_handle> IN it_log_header (  <fs_handle>-log_handle ) ).
      IF lt_log_handle[] IS NOT INITIAL.
        CALL FUNCTION 'BAL_DB_DELETE'
          EXPORTING
*           I_T_LOGS_TO_DELETE =
            i_t_log_handle     = lt_log_handle
*           I_T_LOGNUMBER      =
            i_client           = sy-mandt
*           I_IN_UPDATE_TASK   = ' '
            i_with_commit_work = abap_true
*           I_PACKAGE_SIZE     = 100
          EXCEPTIONS
            no_logs_specified  = 1
            OTHERS             = 2.
        IF sy-subrc <> 0.
* Implement suitable error handling here
          MESSAGE ID sy-msgid TYPE sy-msgty NUMBER sy-msgno
                  WITH sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4.
        ENDIF.
      ENDIF.
    ENDIF.
  ENDMETHOD.


  METHOD bal_db_save.

* Encerrando BALLOG
    DATA(lv_second_connection) = VALUE dbcon_name( ).
    DATA(lt_new) = VALUE bal_t_lgnm( ( )  ).
    DATA(save_all) = abap_true.

    IF im_handle IS NOT INITIAL.
      DATA(handles) = VALUE bal_t_logh( ( im_handle ) ).
      CLEAR save_all.
    ENDIF.

    CALL FUNCTION 'BAL_DB_SAVE'
      EXPORTING
        i_client             = sy-mandt
*       i_in_update_task     = abap_true
        i_save_all           = save_all
        i_t_log_handle       = handles
        i_2th_connection     = abap_true
        i_2th_connect_commit = abap_true
*       I_LINK2JOB           = 'X'
      IMPORTING
        e_new_lognumbers     = lt_new
      EXCEPTIONS
        log_not_found        = 1
        save_not_allowed     = 2
        numbering_error      = 3
        OTHERS               = 4.
    IF sy-subrc <> 0.
      MESSAGE ID sy-msgid
            TYPE sy-msgty
          NUMBER sy-msgno
            WITH sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4
            INTO DATA(lv_dummy).
    ENDIF.

  ENDMETHOD.


  METHOD bal_db_search.

    DATA(is_log_filter) = VALUE bal_s_lfil(
                 object = VALUE bal_r_obj( sign = |I| option = |EQ| ( low = iv_object ) )
              subobject = VALUE bal_r_sub( sign = |I| option = |EQ| ( low = iv_subobject ) ) ).

    CALL FUNCTION 'BAL_DB_SEARCH'
      EXPORTING
        i_client           = sy-mandt
        i_s_log_filter     = is_log_filter
*       I_T_SEL_FIELD      =
*       I_TZONE            =
      IMPORTING
        e_t_log_header     = rt_log_header
      EXCEPTIONS
        log_not_found      = 1
        no_filter_criteria = 2
        OTHERS             = 3.
    IF sy-subrc <> 0.
* Implement suitable error handling here
    ENDIF.
  ENDMETHOD.


  METHOD bal_log_create.

    DATA(ls_log) = VALUE bal_s_log(
                         extnumber = iv_extnumber
                            object = iv_object
                         subobject = iv_subobject
                            aldate = sy-datum
                            altime = sy-uzeit
                            aluser = sy-uname
                            alprog = sy-repid
                        aldate_del = sy-datum + 7
                        del_before = abap_false ).

    CALL FUNCTION 'BAL_LOG_CREATE'
      EXPORTING
        i_s_log      = ls_log
      IMPORTING
        e_log_handle = r_handle
      EXCEPTIONS
        OTHERS       = 1.

  ENDMETHOD.


  METHOD bal_log_hdr_read.

    CHECK NOT iv_handle IS INITIAL.

* are there any messages in this log ?
    CALL FUNCTION 'BAL_LOG_HDR_READ'
      EXPORTING
        i_log_handle = iv_handle
      IMPORTING
        e_statistics = rs_statistics
      EXCEPTIONS
        OTHERS       = 1.

  ENDMETHOD.


  METHOD bal_log_msg_add.

    DATA(ls_log_msg) = VALUE bal_s_msg(
                       msgty = sy-msgty
                       msgid = sy-msgid
                       msgno = sy-msgno
                       msgv1 = sy-msgv1
                       msgv2 = sy-msgv2
                       msgv3 = sy-msgv3
                       msgv4 = sy-msgv4
                     context = VALUE bal_s_cont( tabname = iv_tabname
                                                   value = VALUE bal_s_ex07( sort_field = iv_sort_field
                                                                                  field = iv_field
                                                                             field_text = iv_field_text ) ) ).

    CALL FUNCTION 'BAL_LOG_MSG_ADD'
      EXPORTING
        i_log_handle = iv_handle
        i_s_msg      = ls_log_msg
      EXCEPTIONS
        OTHERS       = 1.

  ENDMETHOD.


  METHOD bal_log_refresh.
    IF NOT iv_handle IS INITIAL.
      CALL FUNCTION 'BAL_LOG_REFRESH'
        EXPORTING
          i_log_handle = iv_handle
        EXCEPTIONS
          OTHERS       = 1.
      IF sy-subrc <> 0.
        MESSAGE ID sy-msgid TYPE sy-msgty NUMBER sy-msgno
                WITH sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4.
      ENDIF.
      r_bool = abap_true.
    ENDIF.
  ENDMETHOD.


  METHOD select_cost_centers.

    SELECT DISTINCT
           kostl, prctr
      INTO TABLE @rt_result
      FROM csks
     WHERE kokrs = 'PWBR'
       AND kostl IN @it_kostl
       AND datbi > @sy-datum.

  ENDMETHOD.


  METHOD bal_db_load.
    DATA:
      msg_handle TYPE bal_t_msgh,
      log_header TYPE balhdr_t.

    DATA(bal_s_lfil) = VALUE bal_s_lfil( log_handle = VALUE bal_r_logh( ( sign = 'I' option = 'EQ' low = iv_handle ) ) ).
    CALL FUNCTION 'BAL_GLB_SEARCH_MSG'
      EXPORTING
        i_s_log_filter = bal_s_lfil
      IMPORTING
        e_t_msg_handle = msg_handle
      EXCEPTIONS
        msg_not_found  = 1
        OTHERS         = 2.
    IF sy-subrc NE 0.
      CALL FUNCTION 'BAL_DB_SEARCH'
        EXPORTING
          i_s_log_filter     = bal_s_lfil
        IMPORTING
          e_t_log_header     = log_header
        EXCEPTIONS
          log_not_found      = 1
          no_filter_criteria = 2
          OTHERS             = 3.
      CALL FUNCTION 'BAL_DB_LOAD'
        EXPORTING
          i_t_log_header     = log_header
        IMPORTING
          e_t_msg_handle     = msg_handle
        EXCEPTIONS
          no_logs_specified  = 1
          log_not_found      = 2
          log_already_loaded = 3
          OTHERS             = 4.
    ENDIF.
  ENDMETHOD.


  METHOD bal_db_read.

    DATA:
      e_s_log                  TYPE bal_s_log,
      e_exists_on_db           TYPE boolean,
      e_created_in_client      TYPE sy-mandt,
      e_saved_in_client        TYPE sy-mandt,
      e_is_modified            TYPE boolean,
      e_lognumber              TYPE balognr,
      e_statistics             TYPE bal_s_scnt,
      e_txt_object             TYPE c,
      e_txt_subobject          TYPE c,
      e_txt_altcode            TYPE c,
      e_txt_almode             TYPE c,
      e_txt_alstate            TYPE c,
      e_txt_probclass          TYPE c,
      e_txt_del_before         TYPE c,
      e_warning_text_not_found TYPE boolean.

    CALL FUNCTION 'BAL_LOG_HDR_READ'
      EXPORTING
        i_log_handle             = i_log_handle
        i_langu                  = sy-langu
      IMPORTING
        e_s_log                  = rs_log
        e_exists_on_db           = e_exists_on_db
        e_created_in_client      = e_created_in_client
        e_saved_in_client        = e_saved_in_client
        e_is_modified            = e_is_modified
        e_lognumber              = e_lognumber
        e_statistics             = e_statistics
        e_txt_object             = e_txt_object
        e_txt_subobject          = e_txt_subobject
        e_txt_altcode            = e_txt_altcode
        e_txt_almode             = e_txt_almode
        e_txt_alstate            = e_txt_alstate
        e_txt_probclass          = e_txt_probclass
        e_txt_del_before         = e_txt_del_before
        e_warning_text_not_found = e_warning_text_not_found
      EXCEPTIONS
        log_not_found            = 1
        OTHERS                   = 2.
    IF sy-subrc <> 0.
* Implement suitable error handling here
      MESSAGE ID sy-msgid
            TYPE sy-msgty
          NUMBER sy-msgno
            WITH sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4.
    ENDIF.
  ENDMETHOD.


  METHOD bal_log_hdr_change.
    CALL FUNCTION 'BAL_LOG_HDR_CHANGE'
      EXPORTING
        i_log_handle            = i_log_handle
        i_s_log                 = i_s_log
      EXCEPTIONS
        log_not_found           = 1
        log_header_inconsistent = 2
        OTHERS                  = 3.
    IF sy-subrc <> 0.
* Implement suitable error handling here
      MESSAGE ID sy-msgid
            TYPE sy-msgty
          NUMBER sy-msgno
            WITH sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4.
    ENDIF.
  ENDMETHOD.
ENDCLASS.
