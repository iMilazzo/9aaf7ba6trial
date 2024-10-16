CLASS zcl_rateio_pep_new DEFINITION
  PUBLIC
  CREATE PUBLIC .

  PUBLIC SECTION.

    INTERFACES if_serializable_object .

    TYPES:
      ty_r_rldnr TYPE RANGE OF acdoca-rldnr .
    TYPES:
      ty_r_bukrs TYPE RANGE OF acdoca-rbukrs .
    TYPES:
      ty_r_gjahr TYPE RANGE OF acdoca-gjahr .
    TYPES:
      ty_r_racct TYPE RANGE OF acdoca-racct .
    TYPES:
      ty_r_belnr TYPE RANGE OF acdoca-belnr .
    TYPES:
      ty_r_budat TYPE RANGE OF acdoca-budat .
    TYPES:
      ty_r_blart TYPE RANGE OF acdoca-blart .
    TYPES:
      ty_r_fkart TYPE RANGE OF vbrk-fkart .
    TYPES:
      ty_r_cpudt TYPE RANGE OF bkpf-cpudt .
    TYPES:
      BEGIN OF ty_s_bkpf,
        bukrs     TYPE bkpf-bukrs,
        belnr     TYPE bkpf-belnr,
        gjahr     TYPE bkpf-gjahr,
        blart     TYPE bkpf-blart,
        tcode     TYPE bkpf-tcode,
        awtyp     TYPE bkpf-awtyp,
        awkey     TYPE bkpf-awkey,
        awref_rev TYPE bkpf-awref_rev,
      END OF ty_s_bkpf .
    TYPES:
      ty_t_bkpf TYPE SORTED TABLE OF ty_s_bkpf
                                  WITH UNIQUE KEY bukrs belnr gjahr .
    TYPES:
      BEGIN OF ty_s_bseg,
        bukrs TYPE bseg-bukrs,
        belnr TYPE bseg-belnr,
        gjahr TYPE bseg-gjahr,
        buzei TYPE bseg-buzei,
        projk TYPE bseg-projk,
      END OF ty_s_bseg .
    TYPES:
      ty_t_bseg TYPE SORTED TABLE OF ty_s_bseg
                                  WITH UNIQUE KEY bukrs belnr gjahr buzei .

    DATA ms_documentheader TYPE bapiache09 .
    DATA mt_accountgl TYPE bapiacgl09_tab .
    DATA mt_currency TYPE bapiaccr09_tab .
    DATA mt_return TYPE bapiret2_tab .
    DATA mt_rv_documents TYPE zttps_rateio_pep_new_sorted .
    DATA r_belnr TYPE ty_r_belnr .
    DATA r_blart TYPE ty_r_blart .
    DATA r_cpudt TYPE ty_r_cpudt .
    DATA r_budat TYPE ty_r_budat .
    DATA r_bukrs TYPE ty_r_bukrs .
    DATA r_bukrs_exc TYPE ty_r_bukrs .
    DATA r_gjahr TYPE ty_r_gjahr .
    DATA r_racct_ze TYPE ty_r_racct .
    DATA r_racct_zi TYPE ty_r_racct .
    DATA r_racct_ze2 TYPE ty_r_racct .
    DATA r_rldnr TYPE ty_r_rldnr .
    DATA _historico TYPE boolean .
    DATA _mass TYPE boolean .
    DATA _nozx TYPE boolean .
    DATA _noze TYPE boolean .
    DATA _time TYPE int4 .

    CLASS-METHODS factory
      RETURNING
        VALUE(ro_rateio) TYPE REF TO zcl_rateio_pep_new .
    METHODS apportionment_calculate
      CHANGING
        !ct_documents TYPE zttps_rateio_pep_new_sorted .
    METHODS constructor
      IMPORTING
        !i_background TYPE abap_bool OPTIONAL
        !is_bkpf      TYPE bkpf OPTIONAL
        !it_belnr     TYPE ty_r_belnr OPTIONAL
        !it_bkpf      TYPE ty_t_bkpf OPTIONAL
        !it_bseg      TYPE ty_t_bseg OPTIONAL
        !iv_testrun   TYPE abap_bool OPTIONAL .
    METHODS execute .
    METHODS get_data
      RETURNING
        VALUE(rt_documents) TYPE zttps_rateio_pep_new_salv .
    METHODS select_all .
    METHODS set_filters
      IMPORTING
        !it_rldnr TYPE ty_r_rldnr OPTIONAL
        !it_bukrs TYPE ty_r_bukrs OPTIONAL
        !it_gjahr TYPE ty_r_gjahr OPTIONAL
        !it_racct TYPE ty_r_racct OPTIONAL
        !it_belnr TYPE ty_r_belnr OPTIONAL
        !it_budat TYPE ty_r_budat OPTIONAL
        !it_blart TYPE ty_r_blart OPTIONAL
        !it_cpudt TYPE ty_r_cpudt OPTIONAL .
    METHODS set_historico
      IMPORTING
        !iv_historico TYPE boolean .
    METHODS set_nozx
      IMPORTING
        !iv_nozx TYPE boolean .
    METHODS set_noze
      IMPORTING
        !iv_noze TYPE boolean .
    METHODS set_mass
      IMPORTING
        !iv_mass TYPE boolean .
    METHODS set_posting_date
      IMPORTING
        !iv_posting_date TYPE acdoca-budat .
  PROTECTED SECTION.

    DATA _0005 TYPE boolean .
    DATA _background TYPE abap_bool .
    DATA _bukrs TYPE bukrs .
    DATA _gjahr TYPE gjahr .
    DATA _handle TYPE balloghndl .
    DATA _has_errors TYPE boolean .
    DATA _posnr TYPE prps-pspnr .
    DATA _pspnr TYPE proj-pspnr .
    DATA _testrun TYPE abap_bool .
    DATA _vbeln TYPE vbeln_vf .
  PRIVATE SECTION.

    TYPES:
      BEGIN OF ty_s_dz_document,
        rldnr      TYPE acdoca-rldnr,
        rbukrs     TYPE acdoca-rbukrs,
        gjahr      TYPE acdoca-gjahr,
        belnr      TYPE acdoca-belnr,
        docln      TYPE acdoca-docln,
        blart      TYPE acdoca-blart,
        rhcur      TYPE acdoca-rhcur,
        hsl        TYPE acdoca-hsl,
        rwcur      TYPE acdoca-rwcur,
        wsl        TYPE acdoca-wsl,
        racct      TYPE acdoca-racct,
        kokrs      TYPE acdoca-kokrs,
        augbl      TYPE acdoca-augbl,
        docorigem  TYPE bkpf-belnr,
        drcrk      TYPE acdoca-drcrk,
        ps_psp_pnr TYPE acdoca-ps_psp_pnr,
        posid_edit TYPE prps-posid_edit,
        ps_prj_pnr TYPE acdoca-ps_prj_pnr,
        pspid_edit TYPE proj-pspid_edit,
        zztp_job   TYPE prps-zztp_job,
        rateio     TYPE bkpf-belnr,
        awtyp      TYPE bkpf-awtyp,
        awkey      TYPE bkpf-awkey,
        awsys      TYPE bkpf-awsys,
        glvor      TYPE bkpf-glvor,
        estorno    TYPE bkpf-belnr,
      END OF ty_s_dz_document .
    TYPES:
      ty_t_dz_documents TYPE SORTED TABLE OF ty_s_dz_document
                            WITH UNIQUE KEY rldnr rbukrs gjahr belnr docln
                            WITH NON-UNIQUE SORTED KEY pspnr COMPONENTS ps_prj_pnr .
    TYPES:
      BEGIN OF ty_s_wbs_element,
        psphi      TYPE prps-psphi,
        pspnr      TYPE prps-pspnr,
        posid      TYPE prps-posid,
        posid_edit TYPE prps-posid_edit,
        zztp_job   TYPE prps-zztp_job,
        usr06      TYPE prps-usr06,
        use06      TYPE prps-use06,
        usr07      TYPE prps-usr07,
      END OF ty_s_wbs_element .
    TYPES:
      ty_t_wbs_elements TYPE SORTED TABLE OF ty_s_wbs_element
                                WITH UNIQUE KEY psphi pspnr posid .
    TYPES:
      BEGIN OF ty_s_acdoca_key,
        rldnr  TYPE acdoca-rldnr,
        rbukrs TYPE acdoca-rbukrs,
        gjahr  TYPE acdoca-gjahr,
        belnr  TYPE acdoca-belnr,
        docln  TYPE acdoca-docln,
        sgtxt  TYPE acdoca-sgtxt,
        xblnr  TYPE bkpf-xblnr,
        bktxt  TYPE bkpf-bktxt,
      END OF ty_s_acdoca_key .
    TYPES:
      ty_t_acdoca_keys TYPE SORTED TABLE OF ty_s_acdoca_key
                                    WITH UNIQUE KEY rldnr rbukrs gjahr belnr docln
                                    WITH NON-UNIQUE SORTED KEY sgtxt COMPONENTS sgtxt
                                    WITH NON-UNIQUE SORTED KEY xblnr COMPONENTS rbukrs xblnr
                                    WITH NON-UNIQUE SORTED KEY bktxt COMPONENTS bktxt .
    TYPES:
      BEGIN OF ty_s_reversal,
        awtyp    TYPE bkpf-awtyp,
        awkey    TYPE bkpf-awkey,
        awsys    TYPE bkpf-awsys,
        bukrs    TYPE bkpf-bukrs,
        glvor    TYPE bkpf-glvor,
        ct_bukrs TYPE bkpf-bukrs, "BrunoCappellini-25.04.2024
        ct_belnr TYPE bkpf-belnr, "BrunoCappellini-25.04.2024
        ct_gjahr TYPE bkpf-gjahr, "BrunoCappellini-25.04.2024
      END OF ty_s_reversal .
    TYPES:
      BEGIN OF ty_s_document_id,
        bukrs TYPE bkpf-bukrs,
        belnr TYPE bkpf-belnr,
        gjahr TYPE bkpf-gjahr,
      END OF ty_s_document_id .
    TYPES:
      BEGIN OF ty_s_flow,
        vbeln_k    TYPE vbrk-vbeln,
        belnr_k    TYPE vbrk-belnr,
        fkart_k    TYPE vbrk-fkart,
        aubel_p    TYPE vbrp-aubel,
        vbelv_f1   TYPE vbfa-vbeln,
        vbtyp_n_f1 TYPE vbfa-vbtyp_n,
        vbeln_f1   TYPE vbfa-vbeln,
        vbtyp_v_f1 TYPE vbfa-vbtyp_v,
        vbelv_f2   TYPE vbfa-vbelv,
        belnr_v1   TYPE acdoca-belnr,
        belnr_b1   TYPE bkpf-belnr,
        pspnr_vb   TYPE vbap-ps_psp_pnr,
        belnr_a    TYPE acdoca-belnr,
        docln_a    TYPE acdoca-docln,
        racct_a    TYPE acdoca-racct,
        posnr      TYPE acdoca-ps_psp_pnr,
        psphi      TYPE acdoca-ps_prj_pnr,
        posid_edit TYPE acdoca-ps_posid,
        rateio_a   TYPE acdoca-zz1_rateio_pep_jei,
        usr07      TYPE prps-usr07,
      END OF ty_s_flow .
    TYPES:
      ty_t_flow TYPE SORTED TABLE OF ty_s_flow
                WITH NON-UNIQUE KEY vbeln_k belnr_k fkart_k aubel_p vbelv_f1 vbtyp_n_f1 vbeln_f1 vbtyp_v_f1 vbelv_f2 belnr_v1 belnr_b1
                WITH NON-UNIQUE SORTED KEY psphi COMPONENTS belnr_k psphi posnr .

    DATA r_blarts TYPE ty_r_blart .
*  types:
*    ty_t_acdoca_keys TYPE STANDARD TABLE OF ty_s_acdoca_key with DEFAULT KEY .
    CONSTANTS c_apoio TYPE ztp_job VALUE 'A' ##NO_TEXT.
    CONSTANTS c_bkpf TYPE awtyp VALUE 'BKPF' ##NO_TEXT.
    CONSTANTS c_bkpff TYPE awtyp VALUE 'BKPFF' ##NO_TEXT.
    CONSTANTS c_dz TYPE blart VALUE 'DZ' ##NO_TEXT.
    CONSTANTS c_principal TYPE ztp_job VALUE 'P' ##NO_TEXT.
    CONSTANTS c_rv TYPE blart VALUE 'RV' ##NO_TEXT.
    CONSTANTS c_vbrk TYPE awtyp VALUE 'VBRK' ##NO_TEXT.
    DATA is_document_id TYPE ty_s_document_id .
    DATA ms_bkpf TYPE ty_s_bkpf .
    DATA mt_bkpf TYPE ty_t_bkpf .
    DATA mt_bseg TYPE ty_t_bseg .
    DATA mt_dz_documents TYPE ty_t_dz_documents .
    DATA mt_processed_items TYPE ty_t_acdoca_keys .
    DATA mt_wbs_elements TYPE ty_t_wbs_elements .
    DATA mt_flow TYPE ty_t_flow .
    DATA r_fkart_exc TYPE ty_r_fkart .
    DATA r_fkart_flow TYPE ty_r_fkart .
    DATA r_racct_destino TYPE ty_r_racct .
    DATA _posting_date TYPE acdoca-budat .

    METHODS apportionment_calculate_cr .
    METHODS apportionment_calculate_deb
      CHANGING
        !ct_documents TYPE zttps_rateio_pep_new_sorted .
    METHODS apportionment_calculate_cr_fat
      CHANGING
        !ct_documents TYPE zttps_rateio_pep_new_sorted .
    METHODS apportionment_calculate_reclas .
    METHODS apportionment_check
      IMPORTING
        !is_documentheader TYPE bapiache09
      CHANGING
        !ct_accountgl      TYPE bapiacgl09_tab
        !ct_currencyamount TYPE bapiaccr09_tab
        !ct_extension1     TYPE bapiacextc_tab
      RETURNING
        VALUE(rv_bool)     TYPE boolean .
    METHODS apportionment_post
      IMPORTING
        !is_documentheader TYPE bapiache09
      CHANGING
        !ct_accountgl      TYPE bapiacgl09_tab
        !ct_currencyamount TYPE bapiaccr09_tab
        !ct_extension1     TYPE bapiacextc_tab
      RETURNING
        VALUE(rv_bool)     TYPE boolean .
    METHODS apportionment_reversal_ze_zi
      IMPORTING
        !is_reversal   TYPE ty_s_reversal
      RETURNING
        VALUE(rv_bool) TYPE boolean .
    METHODS apportionment_reversal_zj
      IMPORTING
        !is_document   TYPE ty_s_dz_document
      RETURNING
        VALUE(rv_bool) TYPE boolean .
    METHODS change_document
      IMPORTING
        !iv_bukrs      TYPE bkpf-bukrs
        !iv_belnr      TYPE bkpf-belnr
        !iv_gjahr      TYPE bkpf-gjahr
        !iv_bktxt      TYPE bkpf-bktxt
      RETURNING
        VALUE(rv_bool) TYPE boolean .
    METHODS is_deb_reversal
      RETURNING
        VALUE(rv_bool) TYPE boolean .
    METHODS delete_sd_and_reversed
      CHANGING
        !ch_documents_tab TYPE zttps_rateio_pep_new_sorted .
    METHODS is_running_in_background
      RETURNING
        VALUE(r_background) TYPE abap_bool .
    METHODS select_open_items
      IMPORTING
        !it_bkpf       TYPE zttfi_rateio_keys OPTIONAL
      RETURNING
        VALUE(rv_bool) TYPE abap_bool .
    METHODS select_set
      IMPORTING
        !iv_setname     TYPE setnamenew
      RETURNING
        VALUE(rt_racct) TYPE ty_r_racct .
    METHODS select_wbs_elements .
    METHODS wait_for_document_creation
      RETURNING
        VALUE(r_was_fully_created) TYPE abap_bool .
    METHODS wait_for_dz_documents
      RETURNING
        VALUE(rv_bool) TYPE boolean .
    METHODS wait_for_rv_documents
      RETURNING
        VALUE(rv_bool) TYPE boolean .
    METHODS wait_for_universal_journal .
    METHODS select_flow .
ENDCLASS.



CLASS zcl_rateio_pep_new IMPLEMENTATION.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method ZCL_RATEIO_PEP_NEW->WAIT_FOR_UNIVERSAL_JOURNAL
* +-------------------------------------------------------------------------------------------------+
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD wait_for_universal_journal.

    MESSAGE i037 INTO DATA(dummy).                          "#EC NEEDED
    zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
    zcl_ptool_helper=>bal_db_save( ).

    DATA(timer)  = cl_abap_runtime=>create_hr_timer( ).
    DATA(t1) = timer->get_runtime( ).

    GET TIME STAMP FIELD DATA(lv_time_ini).
    GET TIME STAMP FIELD DATA(lv_time_fim).
    ADD 30 TO lv_time_fim.

    LOG-POINT ID zcr047 FIELDS ms_bkpf.

    WHILE lv_time_ini <= lv_time_fim.

      GET TIME STAMP FIELD lv_time_ini.

      IF ms_bkpf-blart IN r_blarts[].
*      CASE ms_bkpf-blart.
*        WHEN c_rv.
        IF wait_for_rv_documents( ).
          EXIT.
        ENDIF.
*
*        WHEN c_dz.
*          IF wait_for_dz_documents( ).
*            EXIT.
*          ENDIF.
*
*        WHEN OTHERS.
*
*      ENDCASE.
      ENDIF.
    ENDWHILE.

    DATA(t2) = timer->get_runtime( ).
    MESSAGE i099 WITH |{ ( t2 - t1 ) / 1000 }| INTO dummy.  "#EC NEEDED

    LOG-POINT ID zcr047 FIELDS mt_rv_documents mt_dz_documents.

    MESSAGE i038 WITH |{ lines( mt_rv_documents ) + lines( mt_dz_documents ) }| INTO dummy. "#EC NEEDED
    zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
    zcl_ptool_helper=>bal_db_save( ).

  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method ZCL_RATEIO_PEP_NEW->WAIT_FOR_RV_DOCUMENTS
* +-------------------------------------------------------------------------------------------------+
* | [<-()] RV_BOOL                        TYPE        BOOLEAN
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD wait_for_rv_documents.

    SELECT DISTINCT
           a~rldnr, a~rbukrs, a~gjahr, a~belnr, a~docln, a~koart, a~prctr,
           a~xreversed, a~xreversing, CASE WHEN a~xreversing = 'X' AND b~stblg <> ' ' THEN b~stblg ELSE s~belnr END AS stblg,
           a~bschl, b~kursf, a~rwcur, a~wsl, a~rhcur, a~hsl, a~rtcur, a~tsl, a~racct, a~kokrs,
           a~ps_psp_pnr, p~posid_edit, a~ps_prj_pnr, j~pspid_edit,
           v~vbeln, v~fkart,
           z~vbeln AS vbeln_rev, z~fkart AS fkart_rev,
           p~zztp_job,
           a~drcrk,
           a~bldat, a~budat,
           b~bukrs AS rv_bukrs, b~belnr AS rv_belnr, b~gjahr AS rv_gjahr, b~blart AS rv_blart,
           d~bukrs AS ze_bukrs, d~belnr AS ze_belnr, d~gjahr AS ze_gjahr, d~blart AS ze_blart,
           d~awtyp AS ze_awtyp, d~awkey AS ze_awkey, d~awsys AS ze_awsys, d~glvor AS ze_glvor, d~budat AS ze_budat, d~bldat AS ze_bldat,
           o~bukrs AS zi_bukrs, o~belnr AS zi_belnr, o~gjahr AS zi_gjahr, o~blart AS zi_blart,
           o~awtyp AS zi_awtyp, o~awkey AS zi_awkey, o~awsys AS zi_awsys, o~glvor AS zi_glvor, o~budat AS zi_budat, o~bldat AS zi_bldat,
           x~bukrs AS zx_bukrs, x~belnr AS zx_belnr, x~gjahr AS zx_gjahr, x~blart AS zx_blart,
           x~awtyp AS zx_awtyp, x~awkey AS zx_awkey, x~awsys AS zx_awsys, x~glvor AS zx_glvor, x~budat AS zx_budat, x~bldat AS zx_bldat
      FROM bkpf        AS b
      JOIN acdoca      AS a ON a~rbukrs = b~bukrs
                           AND a~belnr = b~belnr
                           AND a~gjahr = b~gjahr
      LEFT JOIN prps   AS p ON p~pspnr = a~ps_psp_pnr
      LEFT JOIN proj   AS j ON j~pspnr = a~ps_prj_pnr
      JOIN vbrk        AS v ON v~bukrs = a~rbukrs
                           AND ( v~belnr = a~belnr OR v~belnr = b~xblnr )
*                      AND v~fkart   NOT IN ( 'ZDRB' )
      LEFT JOIN vbrk   AS z ON z~vbeln = v~sfakn
                           AND z~fkart IN ( 'ZCRD', 'ZDRB' )
      LEFT JOIN vbrk   AS s ON s~bukrs = a~rbukrs
                           AND s~vbeln = a~awref_rev
      LEFT JOIN bkpf   AS d ON d~bktxt = concat( CASE WHEN a~xreversing = 'X' THEN CASE WHEN b~stblg <> ' ' THEN b~stblg ELSE s~belnr END ELSE b~belnr END, concat( b~bukrs, b~gjahr ) )
                           AND d~blart = 'ZE'
                           AND ( d~stblg = @abap_false AND d~xreversal = @abap_false AND d~xreversing = @abap_false )
      LEFT JOIN bkpf   AS o ON o~bktxt = concat( CASE WHEN a~xreversing = 'X' THEN CASE WHEN b~stblg <> ' ' THEN b~stblg ELSE s~belnr END ELSE b~belnr END, concat( b~bukrs, b~gjahr ) )
                           AND o~blart = 'ZI'
                           AND o~xreversed = @abap_false
      LEFT JOIN bkpf   AS x ON x~bktxt = concat( @ms_bkpf-belnr, concat( b~bukrs, b~gjahr ) )
                           AND x~blart = 'ZX'
                           AND x~xreversed = @abap_false
     WHERE a~rldnr   = '0L'
       AND b~awkey   = @ms_bkpf-awkey
       AND b~bukrs   = @ms_bkpf-bukrs
       AND b~gjahr   = @ms_bkpf-gjahr
       AND ( b~awtyp = @c_vbrk OR b~awtyp = @c_bkpf OR b~awtyp = @c_bkpff )
     ORDER BY a~rldnr, a~rbukrs, a~gjahr, a~belnr, a~docln
      INTO TABLE @mt_rv_documents.

*    delete_sd_and_reversed( CHANGING ch_documents_tab = mt_rv_documents ).=

    IF mt_rv_documents[] IS NOT INITIAL.
*    IF sy-subrc = 0.
      rv_bool = abap_true.
    ENDIF.

  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method ZCL_RATEIO_PEP_NEW->WAIT_FOR_DZ_DOCUMENTS
* +-------------------------------------------------------------------------------------------------+
* | [<-()] RV_BOOL                        TYPE        BOOLEAN
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD wait_for_dz_documents.

    WITH

      +pep AS ( SELECT d~rbukrs, d~belnr, d~gjahr,
                       MAX( d~ps_psp_pnr ) AS ps_psp_pnr,
                       MAX( d~ps_posid ) AS ps_posid ,
                       MAX( d~ps_prj_pnr ) AS ps_prj_pnr,
                       MAX( d~ps_pspid ) AS ps_pspid
                  FROM acdoca AS d
                 GROUP BY d~rbukrs, d~belnr, d~gjahr ),

      +result( rldnr, rbukrs, gjahr, belnr, docln,
               blart, rhcur, hsl, rwcur, wsl, racct, kokrs,
               augbl,
               docorigem,
               drcrk,
               ps_psp_pnr, ps_posid, ps_prj_pnr, ps_pspid,
               zztp_job,
               rateio, awtyp, awkey, awsys, glvor,
               estorno
             ) AS (

      SELECT DISTINCT
             a~rldnr, a~rbukrs, a~gjahr, a~belnr, a~docln,
             a~blart, a~rhcur, a~hsl, a~rwcur, a~wsl, a~racct, a~kokrs,
             a~augbl,
             coalesce( c~belnr, k~belnr ) AS docorigem,
             a~drcrk,
             e~ps_psp_pnr, p~posid_edit, e~ps_prj_pnr, j~pspid_edit,
             p~zztp_job,
             r~belnr, r~awtyp, r~awkey, r~awsys, r~glvor,
             s~belnr
        FROM bkpf AS b
        JOIN acdoca AS a      ON a~rldnr  = '0L'
                             AND a~rbukrs = b~bukrs
                             AND a~gjahr = b~gjahr
                             AND a~belnr = b~belnr
        LEFT JOIN acdoca AS c ON c~rldnr = '0L'
                             AND c~rbukrs = b~bukrs
                             AND c~augbl = b~belnr
                             AND c~auggj = b~gjahr
                             AND c~augdt = b~budat
                             AND c~blart = 'RV'
        LEFT JOIN bkpf AS k   ON k~bukrs = substring( b~bktxt, 11, 4 )
                             AND k~belnr = substring( b~bktxt, 1, 10 )
                             AND k~gjahr = substring( b~bktxt, 15, 4 )
                             AND k~blart = 'RV'
        LEFT JOIN +pep   AS e ON e~rbukrs = b~bukrs
                             AND e~belnr = coalesce( c~belnr, k~belnr )
                             AND e~gjahr = b~gjahr
                             AND e~ps_psp_pnr IS NOT NULL
        LEFT JOIN prps   AS p ON p~pspnr = e~ps_psp_pnr
        LEFT JOIN proj   AS j ON j~pspnr = e~ps_prj_pnr
        LEFT JOIN bkpf   AS r ON r~bukrs = b~bukrs
                             AND r~bktxt = concat( a~awref_rev, concat( a~rbukrs, a~gjahr ) )
                             AND r~blart = 'ZJ'
        LEFT JOIN bkpf   AS s ON s~bukrs = b~bukrs
                             AND s~bktxt = concat( @ms_bkpf-belnr, concat( b~bukrs, b~gjahr ) )
                             AND s~blart = 'EJ'
       WHERE b~bukrs = @ms_bkpf-bukrs
         AND ( b~belnr = @ms_bkpf-belnr OR b~awkey = concat( @ms_bkpf-belnr, concat( @ms_bkpf-bukrs, @ms_bkpf-gjahr ) ) )
         AND b~gjahr = @ms_bkpf-gjahr
         AND a~racct IN @r_racct_ze
         AND ( a~awtyp = 'VBRK' OR a~awtyp = 'BKPFF' OR a~awtyp = 'BKPF' ) )

     SELECT * FROM +result INTO TABLE @mt_dz_documents.

    IF sy-subrc = 0.
      rv_bool = abap_true.
    ENDIF.

  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method ZCL_RATEIO_PEP_NEW->WAIT_FOR_DOCUMENT_CREATION
* +-------------------------------------------------------------------------------------------------+
* | [<-()] R_WAS_FULLY_CREATED            TYPE        ABAP_BOOL
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD wait_for_document_creation.

    MESSAGE i046 INTO DATA(dummy) WITH ms_bkpf-bukrs ms_bkpf-belnr ms_bkpf-gjahr. "#EC NEEDED
    zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
    zcl_ptool_helper=>bal_db_save( ).

    DATA(timer)  = cl_abap_runtime=>create_hr_timer( ).
    DATA(t1) = timer->get_runtime( ).

    GET TIME STAMP FIELD DATA(lv_time_ini).
    GET TIME STAMP FIELD DATA(lv_time_fim).
    ADD 30 TO lv_time_fim.

    WHILE lv_time_ini <= lv_time_fim.

      GET TIME STAMP FIELD lv_time_ini.

      IF _vbeln IS NOT INITIAL.
        SELECT bukrs, belnr, gjahr, awtyp, awkey, awref_rev
          FROM bkpf
         WHERE bukrs = @_bukrs
           AND gjahr = @_gjahr
           AND awtyp = @c_vbrk
           AND awkey = @_vbeln
          INTO @ms_bkpf  UP TO 1 ROWS.
        ENDSELECT.
        IF ms_bkpf-awkey = _vbeln.
          LOG-POINT ID zcr047 FIELDS _bukrs _gjahr _vbeln ms_bkpf.
          EXIT.
        ENDIF.
      ENDIF.
*      IF mt_bkpf[] IS NOT INITIAL.
*        SELECT COUNT(*)
*          FROM @mt_bkpf AS i
*          JOIN bkpf AS k ON k~bukrs = i~bukrs
*                        AND k~belnr = i~belnr
*                        AND k~gjahr = i~gjahr
*          INTO @DATA(count_bkpf).
*        IF count_bkpf = lines( mt_bkpf ).
*          LOG-POINT ID zcr047 FIELDS count_bkpf mt_bkpf.
*          EXIT.
*        ENDIF.
*      ENDIF.

    ENDWHILE.

    DATA(t2) = timer->get_runtime( ).
    MESSAGE i099 WITH |{ ( t2 - t1 ) / 1000 }| INTO dummy.  "#EC NEEDED

    zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
    zcl_ptool_helper=>bal_db_save( ).

    IF ms_bkpf-awkey = _vbeln.
      r_was_fully_created = abap_true.
    ENDIF.


  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Public Method ZCL_RATEIO_PEP_NEW->SET_POSTING_DATE
* +-------------------------------------------------------------------------------------------------+
* | [--->] IV_POSTING_DATE                TYPE        ACDOCA-BUDAT
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD set_posting_date.
    _posting_date = iv_posting_date.
  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Public Method ZCL_RATEIO_PEP_NEW->SET_NOZX
* +-------------------------------------------------------------------------------------------------+
* | [--->] IV_NOZX                        TYPE        BOOLEAN
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD set_nozx.
    _nozx = iv_nozx.
  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Public Method ZCL_RATEIO_PEP_NEW->SET_NOZE
* +-------------------------------------------------------------------------------------------------+
* | [--->] IV_NOZE                        TYPE        BOOLEAN
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD set_noze.
    _noze = iv_noze.
  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Public Method ZCL_RATEIO_PEP_NEW->SET_MASS
* +-------------------------------------------------------------------------------------------------+
* | [--->] IV_MASS                        TYPE        BOOLEAN
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD set_mass.
    _mass = iv_mass.
  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Public Method ZCL_RATEIO_PEP_NEW->SET_HISTORICO
* +-------------------------------------------------------------------------------------------------+
* | [--->] IV_HISTORICO                   TYPE        BOOLEAN
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD set_historico.
    _historico = iv_historico.
  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Public Method ZCL_RATEIO_PEP_NEW->SET_FILTERS
* +-------------------------------------------------------------------------------------------------+
* | [--->] IT_RLDNR                       TYPE        TY_R_RLDNR(optional)
* | [--->] IT_BUKRS                       TYPE        TY_R_BUKRS(optional)
* | [--->] IT_GJAHR                       TYPE        TY_R_GJAHR(optional)
* | [--->] IT_RACCT                       TYPE        TY_R_RACCT(optional)
* | [--->] IT_BELNR                       TYPE        TY_R_BELNR(optional)
* | [--->] IT_BUDAT                       TYPE        TY_R_BUDAT(optional)
* | [--->] IT_BLART                       TYPE        TY_R_BLART(optional)
* | [--->] IT_CPUDT                       TYPE        TY_R_CPUDT(optional)
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD set_filters.
    r_rldnr = it_rldnr[].
    r_bukrs = it_bukrs[].
    r_gjahr = it_gjahr[].
    r_racct_ze = it_racct[].
    r_belnr = it_belnr[].
    r_budat = it_budat[].
    r_blart = it_blart[].
    r_cpudt = it_cpudt[]. "BrunoCappellini-17.06.2024
  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method ZCL_RATEIO_PEP_NEW->SELECT_WBS_ELEMENTS
* +-------------------------------------------------------------------------------------------------+
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD select_wbs_elements.

    MESSAGE i039 INTO DATA(dummy).                          "#EC NEEDED
    zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
    zcl_ptool_helper=>bal_db_save( ).

    DATA(timer)  = cl_abap_runtime=>create_hr_timer( ).
    DATA(t1) = timer->get_runtime( ).

    IF mt_rv_documents IS NOT INITIAL.
      SELECT DISTINCT
             p~psphi, p~pspnr, p~posid, p~posid_edit, p~zztp_job,
             p~usr06, p~use06, p~usr07
        FROM @mt_rv_documents AS i
        JOIN prps AS p ON p~psphi = i~ps_prj_pnr
                      AND p~zztp_job IN ( @c_principal, @c_apoio )
       WHERE usr07 IS NOT INITIAL
       ORDER BY psphi, pspnr, posid
        INTO TABLE @mt_wbs_elements.
    ENDIF.

    IF mt_dz_documents IS NOT INITIAL.
      SELECT DISTINCT
             p~psphi, p~pspnr, p~posid, p~posid_edit, p~zztp_job,
             p~usr06, p~use06, p~usr07
        FROM @mt_dz_documents AS i
        JOIN prps AS p ON p~psphi = i~ps_prj_pnr
                      AND p~zztp_job IN ( @c_principal, @c_apoio )
       WHERE usr07 IS NOT INITIAL
       ORDER BY psphi, pspnr, posid
        INTO TABLE @mt_wbs_elements.
    ENDIF.

    DATA(t2) = timer->get_runtime( ).
    MESSAGE i099 WITH |{ ( t2 - t1 ) / 1000 }| INTO dummy.  "#EC NEEDED

    LOG-POINT ID zcr047 FIELDS mt_wbs_elements.

    MESSAGE i040 WITH |{ lines( mt_wbs_elements ) }| INTO dummy. "#EC NEEDED
    zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
    zcl_ptool_helper=>bal_db_save( ).

  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method ZCL_RATEIO_PEP_NEW->SELECT_SET
* +-------------------------------------------------------------------------------------------------+
* | [--->] IV_SETNAME                     TYPE        SETNAMENEW
* | [<-()] RT_RACCT                       TYPE        TY_R_RACCT
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD select_set.
    SELECT valsign, valoption, CAST( valfrom AS CHAR( 10 ) ) AS low, CAST( valto AS CHAR( 10 ) ) AS high
      FROM setleaf
     WHERE setname = @iv_setname
      INTO TABLE @rt_racct.
  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method ZCL_RATEIO_PEP_NEW->SELECT_OPEN_ITEMS
* +-------------------------------------------------------------------------------------------------+
* | [--->] IT_BKPF                        TYPE        ZTTFI_RATEIO_KEYS(optional)
* | [<-()] RV_BOOL                        TYPE        ABAP_BOOL
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD select_open_items.

    MESSAGE i037 INTO DATA(dummy).                          "#EC NEEDED
    zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
    zcl_ptool_helper=>bal_db_save( ).

    IF it_bkpf IS SUPPLIED.
      mt_bkpf = CORRESPONDING #( it_bkpf ).
    ENDIF.

    DATA(timer)  = cl_abap_runtime=>create_hr_timer( ).
    DATA(t1) = timer->get_runtime( ).

    SELECT DISTINCT
           a~rldnr, a~rbukrs, a~gjahr, a~belnr, a~docln, a~koart, a~prctr,
           a~xreversed, a~xreversing, CASE WHEN a~xreversing = 'X' AND b~stblg <> ' ' THEN b~stblg ELSE s~belnr END AS stblg,
           a~bschl, b~kursf, a~rwcur, a~wsl, a~rhcur, a~hsl, a~rtcur, a~tsl, a~racct, a~kokrs,
           a~ps_psp_pnr, p~posid_edit, a~ps_prj_pnr, j~pspid_edit,
           v~vbeln, v~fkart,
           z~vbeln AS vbeln_rev, z~fkart AS fkart_rev,
           p~zztp_job,
           a~drcrk,
           a~bldat, a~budat,
           b~bukrs AS rv_bukrs, b~belnr AS rv_belnr, b~gjahr AS rv_gjahr, b~blart AS rv_blart,
           d~bukrs AS ze_bukrs, d~belnr AS ze_belnr, d~gjahr AS ze_gjahr, d~blart AS ze_blart,
           d~awtyp AS ze_awtyp, d~awkey AS ze_awkey, d~awsys AS ze_awsys, d~glvor AS ze_glvor, d~budat AS ze_budat, d~bldat AS ze_bldat,
           o~bukrs AS zi_bukrs, o~belnr AS zi_belnr, o~gjahr AS zi_gjahr, o~blart AS zi_blart,
           o~awtyp AS zi_awtyp, o~awkey AS zi_awkey, o~awsys AS zi_awsys, o~glvor AS zi_glvor, o~budat AS zi_budat, o~bldat AS zi_bldat,
           x~bukrs AS zx_bukrs, x~belnr AS zx_belnr, x~gjahr AS zx_gjahr, x~blart AS zx_blart,
           x~awtyp AS zx_awtyp, x~awkey AS zx_awkey, x~awsys AS zx_awsys, x~glvor AS zx_glvor, o~budat AS zx_budat, x~bldat AS zx_bldat
      FROM @mt_bkpf    AS i
      JOIN bkpf        AS b ON b~awkey = i~awkey
                           AND b~bukrs = i~bukrs
                           AND b~gjahr = i~gjahr
      JOIN acdoca      AS a ON a~rbukrs = b~bukrs
                           AND a~belnr = b~belnr
                           AND a~gjahr = b~gjahr
      LEFT JOIN prps   AS p ON p~pspnr = a~ps_psp_pnr
      LEFT JOIN proj   AS j ON j~pspnr = a~ps_prj_pnr
      LEFT JOIN vbrk   AS v ON v~bukrs = a~rbukrs
                           AND ( v~belnr = a~belnr OR v~belnr = b~xblnr )
      LEFT JOIN vbrk   AS s ON s~bukrs = a~rbukrs
                           AND s~vbeln = a~awref_rev
      LEFT JOIN vbrk   AS z ON z~vbeln = v~sfakn
                           AND z~fkart IN ( 'ZCRD', 'ZDRB' )
      LEFT JOIN bkpf   AS d ON d~bktxt = concat( CASE WHEN a~xreversing = 'X' THEN CASE WHEN b~stblg <> ' ' THEN b~stblg ELSE s~belnr END ELSE b~belnr END, concat( b~bukrs, b~gjahr ) )
                           AND d~blart = 'ZE'
                           AND ( d~stblg = @abap_false AND d~xreversal = @abap_false AND d~xreversing = @abap_false )
      LEFT JOIN bkpf   AS o ON o~bktxt = concat( CASE WHEN a~xreversing = 'X' THEN CASE WHEN b~stblg <> ' ' THEN b~stblg ELSE s~belnr END ELSE b~belnr END, concat( b~bukrs, b~gjahr ) )
                           AND o~blart = 'ZI'
                           AND o~xreversed = @abap_false
      LEFT JOIN bkpf   AS x ON x~bktxt = concat( CASE WHEN a~xreversing = 'X' THEN CASE WHEN b~stblg <> ' ' THEN b~stblg ELSE s~belnr END ELSE b~belnr END, concat( b~bukrs, b~gjahr ) )
                           AND x~blart = 'ZX'
                           AND x~xreversed = @abap_false
     WHERE a~rldnr   = '0L'
       AND ( b~awtyp = @c_vbrk OR b~awtyp = @c_bkpf OR b~awtyp = @c_bkpff )
     ORDER BY a~rldnr, a~rbukrs, a~gjahr, a~belnr, a~docln
      INTO TABLE @mt_rv_documents.

    IF _nozx = abap_true.
      DATA(rv_document) = VALUE zesps_rateio_pep_new_salv( ).
      MODIFY mt_rv_documents FROM rv_document TRANSPORTING zx_bukrs zx_belnr zx_gjahr zx_blart zx_awtyp zx_awkey zx_awsys zx_glvor zx_budat zx_bldat WHERE belnr IS NOT INITIAL.
      DELETE ADJACENT DUPLICATES FROM mt_rv_documents COMPARING rldnr rbukrs gjahr belnr docln.
    ENDIF.

*    DELETE ADJACENT DUPLICATES FROM results COMPARING rldnr rbukrs gjahr belnr docln.
*    mt_rv_documents = CORRESPONDING #( results ).

    IF mt_rv_documents IS NOT INITIAL.
      rv_bool = abap_true.
    ENDIF.

    DATA(t2) = timer->get_runtime( ).
    MESSAGE i099 WITH |{ ( t2 - t1 ) / 1000 }| INTO dummy.  "#EC NEEDED

    LOG-POINT ID zcr047 FIELDS mt_rv_documents.

    MESSAGE i038 WITH |{ lines( mt_rv_documents ) }| INTO dummy. "#EC NEEDED
    zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
    zcl_ptool_helper=>bal_db_save( ).

  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method ZCL_RATEIO_PEP_NEW->SELECT_FLOW
* +-------------------------------------------------------------------------------------------------+
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD select_flow.

    SELECT DISTINCT
           k~vbeln, k~belnr, k~fkart, p~aubel,
           f1~vbelv, f1~vbtyp_n, f1~vbeln, f1~vbtyp_v,
           f2~vbelv, v1~belnr, b1~belnr, vb~ps_psp_pnr,
           a~belnr, a~docln, a~racct, a~ps_psp_pnr, a~ps_prj_pnr, a~ps_posid, a~zz1_rateio_pep_jei,
           division( a~zz1_rateio_pep_jei, 10, 3 )
      FROM @mt_rv_documents AS i
      JOIN vbrk   AS k  ON k~belnr = i~belnr
                       AND k~bukrs = i~rbukrs
                       AND k~gjahr = i~gjahr
      JOIN vbrp   AS p  ON p~vbeln = k~vbeln
      JOIN vbfa   AS f1 ON f1~vbeln = k~vbeln
                       AND f1~vbtyp_v = 'M'
      JOIN vbfa   AS f2 ON f2~vbeln = f1~vbelv
      JOIN vbap   AS vb ON vb~vbeln = f2~vbelv
      JOIN vbrk   AS v1 ON v1~vbeln = f1~vbelv
      JOIN bkpf   AS b1 ON b1~bktxt = concat( v1~belnr, concat( v1~bukrs, v1~gjahr ) )
                       AND b1~blart = 'ZE'
      JOIN acdoca AS a  ON a~rldnr = '0L'
                       AND a~rbukrs = b1~bukrs
                       AND a~belnr = b1~belnr
                       AND a~gjahr = b1~gjahr
     WHERE k~fkart IN @r_fkart_flow
       AND vb~ps_psp_pnr <> '00000000'
      INTO TABLE @mt_flow.
    DELETE ADJACENT DUPLICATES FROM mt_flow COMPARING ALL FIELDS.
  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Public Method ZCL_RATEIO_PEP_NEW->SELECT_ALL
* +-------------------------------------------------------------------------------------------------+
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD select_all.
    select_open_items( ).
    select_wbs_elements( ).
    r_racct_destino = select_set( 'RATEIO_PEP_DESTINO_NOVO' ).
    r_racct_ze = select_set( 'RATEIO_PEP_NOVO' ).
    r_racct_zi = select_set( 'RATEIO_PEP_ZI' ).
    r_bukrs_exc = select_set( 'RATEIO_EXC_ZI' ).
    SELECT sign, opti, low, high FROM tvarvc WHERE name = 'ZSD_EXCLUIR_FAT_AJUSTE_0005' AND type = 'S' INTO TABLE @r_fkart_exc.
    SELECT SINGLE low INTO _0005 FROM tvarvc WHERE name = 'ZSD_RATEIO_0005_ZI' AND type = 'P' AND numb = '0000'.
  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method ZCL_RATEIO_PEP_NEW->IS_RUNNING_IN_BACKGROUND
* +-------------------------------------------------------------------------------------------------+
* | [<-()] R_BACKGROUND                   TYPE        ABAP_BOOL
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD is_running_in_background.
    r_background = _background.
  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method ZCL_RATEIO_PEP_NEW->IS_DEB_REVERSAL
* +-------------------------------------------------------------------------------------------------+
* | [<-()] RV_BOOL                        TYPE        BOOLEAN
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD is_deb_reversal.

    TYPES: BEGIN OF ty_est,
             vbeln_est TYPE vbrk-vbeln,
           END OF ty_est.

    DATA: excluir_estornos TYPE SORTED TABLE OF ty_est WITH UNIQUE KEY vbeln_est.

    "Excluir os documentos do tipo ZDRB
    DATA(documents) = mt_rv_documents[].
    DELETE documents USING KEY fkart WHERE fkart = 'ZDRB'.

    "Busca os documentos de estornos com referência a um ZDRB
    SELECT DISTINCT a~vbeln AS vbeln_est
      FROM @documents AS i
     INNER JOIN vbrk AS a ON a~vbeln = i~vbeln
     INNER JOIN vbrk AS b ON b~vbeln = a~sfakn
                         AND b~fkart = 'ZDRB'
      INTO TABLE @excluir_estornos.

    IF excluir_estornos[] IS NOT INITIAL.
      rv_bool = abap_true.
    ENDIF.

  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Public Method ZCL_RATEIO_PEP_NEW->GET_DATA
* +-------------------------------------------------------------------------------------------------+
* | [<-()] RT_DOCUMENTS                   TYPE        ZTTPS_RATEIO_PEP_NEW_SALV
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD get_data.
    rt_documents = mt_rv_documents.
  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Static Public Method ZCL_RATEIO_PEP_NEW=>FACTORY
* +-------------------------------------------------------------------------------------------------+
* | [<-()] RO_RATEIO                      TYPE REF TO ZCL_RATEIO_PEP_NEW
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD factory.
    ro_rateio = NEW zcl_rateio_pep_new( ).
  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Public Method ZCL_RATEIO_PEP_NEW->EXECUTE
* +-------------------------------------------------------------------------------------------------+
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD execute.

    DATA(timer) = cl_abap_runtime=>create_hr_timer( ).
    DATA(t1) = timer->get_runtime( ).

    r_racct_destino = select_set( 'RATEIO_PEP_DESTINO_NOVO' ).
    r_racct_ze = select_set( 'RATEIO_PEP_NOVO' ).
    r_racct_zi = select_set( 'RATEIO_PEP_ZI' ).
    r_bukrs_exc = select_set( 'RATEIO_EXC_ZI' ).

    "ZSD_FI_RATEIO_PEP_ZE

    SELECT sign, opti, low, high FROM tvarvc WHERE name = 'ZSD_EXCLUIR_FAT_AJUSTE_0005' AND type = 'S' INTO TABLE @r_fkart_exc.
    SELECT sign, opti, low, high FROM tvarvc WHERE name = 'ZSD_APPEND_REF_NC_ZE' AND type = 'S' INTO TABLE @r_fkart_flow.
    SELECT SINGLE low INTO _0005 FROM tvarvc WHERE name = 'ZSD_RATEIO_0005_ZI' AND type = 'P' AND numb = '0000'.
    SELECT sign, opti, low, high FROM tvarvc WHERE name = 'ZSD_FI_RATEIO_PEP_ZE' AND type = 'S' INTO TABLE @r_racct_ze2.
    SELECT sign, opti, low, high FROM tvarvc WHERE name = 'ZSD_FI_BLART_RATEIO' AND type = 'S' INTO TABLE @DATA(rl_blarts).

    IF is_running_in_background( ).
      wait_for_universal_journal( ).
    ELSE.
      select_open_items( ).
    ENDIF.

    IF mt_rv_documents IS NOT INITIAL.
      _pspnr = REDUCE proj-pspid_edit( INIT lv_pspnr TYPE proj-pspnr
                                        FOR <rv> IN FILTER #( mt_rv_documents USING KEY pspnr WHERE ps_prj_pnr <> CONV proj-pspnr( '' ) )
                                       NEXT lv_pspnr = <rv>-ps_prj_pnr ).
    ENDIF.

    IF mt_dz_documents IS NOT INITIAL.
      _pspnr = REDUCE proj-pspid_edit( INIT lv_pspnr TYPE proj-pspnr
                                        FOR <dz> IN FILTER #( mt_rv_documents USING KEY pspnr WHERE ps_prj_pnr <> CONV proj-pspnr( '' ) )
                                       NEXT lv_pspnr = <dz>-ps_prj_pnr ).
    ENDIF.

    select_wbs_elements( ).
    select_flow( ).

    IF mt_rv_documents[] IS NOT INITIAL.
      IF ( line_exists( mt_rv_documents[ KEY fkart fkart = 'ZDRB' ] ) OR
           line_exists( mt_rv_documents[ KEY fkart fkart = 'S1' fkart_rev = 'ZDRB' ] ) ) OR
         ( line_exists( mt_rv_documents[ KEY fkart fkart = 'ZCRD' ] ) OR
           line_exists( mt_rv_documents[ KEY fkart fkart = 'S2' fkart_rev = 'ZCRD' ] ) ).
        apportionment_calculate_deb( CHANGING ct_documents = mt_rv_documents ).
      ELSE.
        IF _noze = abap_false.
          apportionment_calculate( CHANGING ct_documents = mt_rv_documents ).
        ENDIF.
        apportionment_calculate_cr_fat( CHANGING ct_documents = mt_rv_documents ).
      ENDIF.
    ENDIF.

    DATA(t2) = timer->get_runtime( ).
    MESSAGE i047 WITH |{ ( t2 - t1 ) / 1000 }| INTO DATA(dummy). "#EC NEEDED
    zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
    zcl_ptool_helper=>bal_db_save( ).

  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method ZCL_RATEIO_PEP_NEW->DELETE_SD_AND_REVERSED
* +-------------------------------------------------------------------------------------------------+
* | [<-->] CH_DOCUMENTS_TAB               TYPE        ZTTPS_RATEIO_PEP_NEW_SORTED
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD delete_sd_and_reversed.

    TYPES: BEGIN OF ty_est,
             vbeln_est TYPE vbrk-vbeln,
           END OF ty_est.

    DATA: excluir_estornos TYPE SORTED TABLE OF ty_est WITH UNIQUE KEY vbeln_est.

    "Excluir os documentos do tipo ZDRB
    DELETE ch_documents_tab USING KEY fkart WHERE fkart = 'ZDRB'.

    "Busca os documentos de estornos com referência a um ZDRB
    SELECT DISTINCT a~vbeln AS vbeln_est
      FROM @ch_documents_tab AS z
     INNER JOIN vbrk AS a ON a~vbeln = z~vbeln
     INNER JOIN vbrk AS b ON b~vbeln = a~sfakn
                         AND b~fkart = 'ZDRB'
      INTO TABLE @excluir_estornos.

    "Exclui os documentos de estornos com referencia a um ZDRB.
    ch_documents_tab = FILTER #( ch_documents_tab EXCEPT IN excluir_estornos WHERE vbeln = vbeln_est ).

  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Public Method ZCL_RATEIO_PEP_NEW->CONSTRUCTOR
* +-------------------------------------------------------------------------------------------------+
* | [--->] I_BACKGROUND                   TYPE        ABAP_BOOL(optional)
* | [--->] IS_BKPF                        TYPE        BKPF(optional)
* | [--->] IT_BELNR                       TYPE        TY_R_BELNR(optional)
* | [--->] IT_BKPF                        TYPE        TY_T_BKPF(optional)
* | [--->] IT_BSEG                        TYPE        TY_T_BSEG(optional)
* | [--->] IV_TESTRUN                     TYPE        ABAP_BOOL(optional)
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD constructor.
    _background = i_background.
    r_belnr = CORRESPONDING #( it_belnr ).
    mt_bkpf = CORRESPONDING #( it_bkpf ).
    mt_bseg = CORRESPONDING #( it_bseg ).
    ms_bkpf = CORRESPONDING #( is_bkpf ).
    IF ms_bkpf IS NOT INITIAL.
      _vbeln = is_bkpf-awkey.
      _bukrs = is_bkpf-bukrs.
      _gjahr = is_bkpf-gjahr.
    ELSE.
      IF mt_bkpf[] IS NOT INITIAL.
        _vbeln = mt_bkpf[ 1 ]-awkey.
        _bukrs = mt_bkpf[ 1 ]-bukrs.
        _gjahr = mt_bkpf[ 1 ]-gjahr.
      ENDIF.
    ENDIF.
*    IF _mass IS INITIAL.
*      IF cl_os_system=>init_state IS INITIAL.
*        cl_os_system=>init_and_set_modes( i_external_commit = oscon_true i_update_mode = oscon_dmode_update_task  ).
*      ENDIF.
*      "Init Transaction Manager
*      transaction_manager = cl_os_system=>get_transaction_manager( ).
*    ENDIF.
    " Set the test mode
    SELECT SINGLE low
      FROM tvarvc
     WHERE name = 'ZCR47_MODO_TESTE'
       AND type = 'P'
      INTO @_testrun.

    _handle = zcl_ptool_helper=>bal_log_create( EXPORTING iv_object = 'ZPS' iv_subobject = 'RATEIO' ).

    MESSAGE i036 INTO DATA(dummy).                          "#EC NEEDED
    zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
    zcl_ptool_helper=>bal_db_save( ).

    LOG-POINT ID zcr047 FIELDS sy-cprog sy-repid mt_bkpf mt_bseg.
  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method ZCL_RATEIO_PEP_NEW->CHANGE_DOCUMENT
* +-------------------------------------------------------------------------------------------------+
* | [--->] IV_BUKRS                       TYPE        BKPF-BUKRS
* | [--->] IV_BELNR                       TYPE        BKPF-BELNR
* | [--->] IV_GJAHR                       TYPE        BKPF-GJAHR
* | [--->] IV_BKTXT                       TYPE        BKPF-BKTXT
* | [<-()] RV_BOOL                        TYPE        BOOLEAN
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD change_document.

    CHECK iv_bukrs IS NOT INITIAL AND
          iv_belnr IS NOT INITIAL AND
          iv_gjahr IS NOT INITIAL .

    GET TIME STAMP FIELD DATA(lv_time_ini).
    GET TIME STAMP FIELD DATA(lv_time_fim).
    ADD 60 TO lv_time_fim.

    LOG-POINT ID zcr047 FIELDS iv_bukrs
                               iv_belnr
                               iv_gjahr
                               iv_bktxt.

    WHILE lv_time_ini <= lv_time_fim.

      LOG-POINT ID zcr047 FIELDS sy-index.

      GET TIME STAMP FIELD lv_time_ini.

      SELECT bukrs, belnr, gjahr, buzei
        FROM bseg
       WHERE bukrs = @iv_bukrs
         AND belnr = @iv_belnr
         AND gjahr = @iv_gjahr
        INTO TABLE @DATA(bseg).

      IF sy-subrc = 0.
        EXIT.
      ENDIF.
    ENDWHILE.

    rv_bool = abap_true.
    DATA(accchg) = VALUE arberp_t_accchg( ( fdname = 'BKTXT' newval = iv_bktxt )
                                          ( fdname = 'XBLNR' newval = iv_bktxt(10) )
                                          ( fdname = 'SGTXT' newval = iv_bktxt ) ).

    LOOP AT bseg ASSIGNING FIELD-SYMBOL(<bseg>).

      LOG-POINT ID zcr047 FIELDS <bseg> accchg.

      CALL FUNCTION 'FI_DOCUMENT_CHANGE'
        EXPORTING
*         X_LOCK               = 'X'
          i_bukrs              = iv_bukrs
          i_belnr              = iv_belnr
          i_gjahr              = iv_gjahr
          i_buzei              = <bseg>-buzei
        TABLES
          t_accchg             = accchg
        EXCEPTIONS
          no_reference         = 1
          no_document          = 2
          many_documents       = 3
          wrong_input          = 4
          overwrite_creditcard = 5
          OTHERS               = 6.
      IF sy-subrc <> 0.
* Implement suitable error handling here
        LOG-POINT ID zcr047 FIELDS sy-subrc.
*        MESSAGE ID sy-msgid
*              TYPE sy-msgty
*            NUMBER sy-msgno
*              WITH sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4 .
      ELSE.
        LOG-POINT ID zcr047 FIELDS sy-subrc.
*        MESSAGE i092 WITH iv_bukrs iv_belnr iv_gjahr iv_bktxt INTO DATA(dummy). "#EC NEEDED
*        zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
*        zcl_ptool_helper=>bal_db_save( ).
        rv_bool = abap_true.
      ENDIF.
      IF _mass = abap_true.
        CALL FUNCTION 'BAPI_TRANSACTION_COMMIT'
          EXPORTING
            wait = abap_true.
      ENDIF.
    ENDLOOP.
  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method ZCL_RATEIO_PEP_NEW->APPORTIONMENT_REVERSAL_ZJ
* +-------------------------------------------------------------------------------------------------+
* | [--->] IS_DOCUMENT                    TYPE        TY_S_DZ_DOCUMENT
* | [<-()] RV_BOOL                        TYPE        BOOLEAN
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD apportionment_reversal_zj.

    DATA(lt_return) = VALUE bapiret2_tab( ( ) ).
    DATA(obj_type) = VALUE bapiache09-obj_type( ). "#EC NEEDED
    DATA(obj_key) = VALUE bapiache09-obj_key( ).   "#EC NEEDED
    DATA(obj_sys) = VALUE bapiache09-obj_sys( ).   "#EC NEEDED

    DATA(ls_reversal) = VALUE bapiacrev( obj_type = is_document-awtyp
                                          obj_key = is_document-awkey
                                          obj_sys = is_document-awsys
                                        obj_key_r = is_document-awkey
                                        comp_code = is_document-rbukrs
                                       reason_rev = '01' ).

    CALL FUNCTION 'BAPI_ACC_DOCUMENT_REV_POST'
      EXPORTING
        reversal = ls_reversal
        bus_act  = is_document-glvor
      IMPORTING
        obj_type = obj_type
        obj_key  = obj_key
        obj_sys  = obj_sys
      TABLES
        return   = lt_return.

    LOOP AT lt_return ASSIGNING FIELD-SYMBOL(<return>).
      MESSAGE ID <return>-id
         TYPE <return>-type
       NUMBER <return>-number
         WITH <return>-message_v1 <return>-message_v2 <return>-message_v3 <return>-message_v4
         INTO DATA(dummy).                                  "#EC NEEDED
      zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
    ENDLOOP.
    zcl_ptool_helper=>bal_db_save( ).

    IF NOT line_exists( lt_return[ type = CONV bapi_mtype( `E` ) ] ).

      DATA(message) = VALUE #( lt_return[ type = CONV bapi_mtype( `S` )
                                            id = CONV symsgid( 'RW' )
                                        number = CONV symsgno( '605' ) ] OPTIONAL ).
      IF message IS NOT INITIAL.
        is_document_id = VALUE #( belnr = message-message_v2(10)
                                  bukrs = message-message_v2+10(4)
                                  gjahr = message-message_v2+14(4) ) .
        SET PARAMETER ID 'BLN' FIELD is_document_id-belnr.
        SET PARAMETER ID 'BUK' FIELD is_document_id-bukrs.
        SET PARAMETER ID 'GJR' FIELD is_document_id-gjahr.
        MESSAGE ID 'ZPS' TYPE message-type NUMBER '093' WITH message-message_v2(10) message-message_v2+10(4) message-message_v2+14(4) INTO dummy.
        zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
        zcl_ptool_helper=>bal_db_save( ).
        rv_bool = abap_true.
      ENDIF.

    ENDIF.
  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method ZCL_RATEIO_PEP_NEW->APPORTIONMENT_REVERSAL_ZE_ZI
* +-------------------------------------------------------------------------------------------------+
* | [--->] IS_REVERSAL                    TYPE        TY_S_REVERSAL
* | [<-()] RV_BOOL                        TYPE        BOOLEAN
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD apportionment_reversal_ze_zi.

    DATA(obj_type) = VALUE bapiache09-obj_type( ).          "#EC NEEDED
    DATA(obj_key) = VALUE bapiache09-obj_key( ).            "#EC NEEDED
    DATA(obj_sys) = VALUE bapiache09-obj_sys( ).            "#EC NEEDED

    DATA(ls_reversal) = VALUE bapiacrev( obj_type  = is_reversal-awtyp
                                          obj_key  = is_reversal-awkey
                                          obj_sys  = is_reversal-awsys
                                        obj_key_r  = is_reversal-awkey
                                        comp_code  = is_reversal-bukrs
                                       reason_rev  = '01' ).

*** BrunoCappellini-25.04.2024 {
* Resgatar o valor de BKPF-BUDAT -->>
* BAPIACREV-PSTNG_DATE, na criação dos documentos ACDOCA-BLART = EE e EI
    SELECT SINGLE * FROM bkpf WHERE bukrs = @is_reversal-ct_bukrs
                                AND belnr = @is_reversal-ct_belnr
                                AND gjahr = @is_reversal-ct_gjahr
      INTO @DATA(lwa_bkpf).
    IF sy-subrc = 0.
      ls_reversal-pstng_date = lwa_bkpf-budat.
    ENDIF.
*** } BrunoCappellini-25.04.2024

    LOG-POINT ID zcr047 FIELDS is_reversal ls_reversal.

    CALL FUNCTION 'BAPI_ACC_DOCUMENT_REV_POST'
      EXPORTING
        reversal = ls_reversal
        bus_act  = is_reversal-glvor
      IMPORTING
        obj_type = obj_type
        obj_key  = obj_key
        obj_sys  = obj_sys
      TABLES
        return   = mt_return.

    LOOP AT mt_return ASSIGNING FIELD-SYMBOL(<return>).
      MESSAGE ID <return>-id
         TYPE <return>-type
       NUMBER <return>-number
         WITH <return>-message_v1 <return>-message_v2 <return>-message_v3 <return>-message_v4
         INTO DATA(dummy).                                  "#EC NEEDED
      zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
      zcl_ptool_helper=>bal_db_save( ).
    ENDLOOP.

    LOG-POINT ID zcr047 FIELDS mt_return.

    IF NOT line_exists( mt_return[ type = CONV bapi_mtype( `E` ) ] ).

      DATA(message) = VALUE #( mt_return[ type = CONV bapi_mtype( `S`  )
                                            id = CONV symsgid( 'RW' )
                                        number = CONV symsgno( '605' ) ] OPTIONAL ).
      LOG-POINT ID zcr047 FIELDS message.
      IF message IS NOT INITIAL.
        is_document_id = VALUE #( belnr = message-message_v2(10)
                                  bukrs = message-message_v2+10(4)
                                  gjahr = message-message_v2+14(4) ) .
        SET PARAMETER ID 'BLN' FIELD is_document_id-belnr.
        SET PARAMETER ID 'BUK' FIELD is_document_id-bukrs.
        SET PARAMETER ID 'GJR' FIELD is_document_id-gjahr.
        LOG-POINT ID zcr047 FIELDS is_document_id.
        MESSAGE ID 'ZPS' TYPE message-type NUMBER '093' WITH message-message_v2(10) message-message_v2+10(4) message-message_v2+14(4) INTO dummy.
        zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
        zcl_ptool_helper=>bal_db_save( ).
        rv_bool = abap_true.
      ENDIF.

      IF ( _historico = abap_true OR _mass = abap_true ) AND rv_bool = abap_true.
        CALL FUNCTION 'BAPI_TRANSACTION_COMMIT'
          EXPORTING
            wait = abap_true.
      ENDIF.

    ENDIF.
  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method ZCL_RATEIO_PEP_NEW->APPORTIONMENT_POST
* +-------------------------------------------------------------------------------------------------+
* | [--->] IS_DOCUMENTHEADER              TYPE        BAPIACHE09
* | [<-->] CT_ACCOUNTGL                   TYPE        BAPIACGL09_TAB
* | [<-->] CT_CURRENCYAMOUNT              TYPE        BAPIACCR09_TAB
* | [<-->] CT_EXTENSION1                  TYPE        BAPIACEXTC_TAB
* | [<-()] RV_BOOL                        TYPE        BOOLEAN
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD apportionment_post.

    MESSAGE i044 INTO DATA(dummy).                          "#EC NEEDED
    zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
    zcl_ptool_helper=>bal_db_save( ).

    DATA(obj_type) = VALUE bapiache09-obj_type( ).          "#EC NEEDED
    DATA(obj_key) = VALUE bapiache09-obj_key( ).            "#EC NEEDED
    DATA(obj_sys) = VALUE bapiache09-obj_sys( ).            "#EC NEEDED

*** BrunoCappellini-23.04.2024 {
    LOOP AT ct_accountgl ASSIGNING FIELD-SYMBOL(<fs_accountgl>) WHERE alloc_nmbr IS INITIAL.
      <fs_accountgl>-alloc_nmbr = 'R'.
    ENDLOOP.
*** } BrunoCappellini-23.04.2024

    CALL FUNCTION 'BAPI_ACC_DOCUMENT_POST'
      EXPORTING
        documentheader = is_documentheader
      IMPORTING
        obj_type       = obj_type
        obj_key        = obj_key
        obj_sys        = obj_sys
      TABLES
        accountgl      = ct_accountgl
        currencyamount = ct_currencyamount
        extension1     = ct_extension1
        return         = mt_return.

    LOG-POINT ID zcr047 FIELDS obj_type obj_key obj_sys mt_return .

    LOOP AT mt_return ASSIGNING FIELD-SYMBOL(<return>).
      MESSAGE ID <return>-id
         TYPE <return>-type
       NUMBER <return>-number
         WITH <return>-message_v1 <return>-message_v2 <return>-message_v3 <return>-message_v4
         INTO dummy.
      zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
    ENDLOOP.
    zcl_ptool_helper=>bal_db_save( ).

    IF NOT line_exists( mt_return[ type = CONV bapi_mtype( `E` ) ] ).

      DATA(message) = VALUE #( mt_return[ type = CONV bapi_mtype( `S` )
                                            id = CONV symsgid( 'RW' )
                                        number = CONV symsgno( '605' ) ] OPTIONAL ).
      IF message IS NOT INITIAL.
        SET PARAMETER ID 'BLN' FIELD message-message_v2(10).
        SET PARAMETER ID 'BUK' FIELD message-message_v2+10(4).
        SET PARAMETER ID 'GJR' FIELD message-message_v2+14(4).
        MESSAGE ID 'ZPS' TYPE message-type NUMBER message-number WITH message-message_v2(10) message-message_v2+10(4) message-message_v2+14(4) INTO dummy.
        zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
        zcl_ptool_helper=>bal_db_save( ).
        rv_bool = abap_true.
      ENDIF.

      IF _mass IS NOT INITIAL AND rv_bool = abap_true.

        CALL FUNCTION 'BAPI_TRANSACTION_COMMIT'
          EXPORTING
            wait = abap_true
*       IMPORTING
*           RETURN        =
          .
      ENDIF.
    ELSE.
      _has_errors = abap_true.
    ENDIF.

  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method ZCL_RATEIO_PEP_NEW->APPORTIONMENT_CHECK
* +-------------------------------------------------------------------------------------------------+
* | [--->] IS_DOCUMENTHEADER              TYPE        BAPIACHE09
* | [<-->] CT_ACCOUNTGL                   TYPE        BAPIACGL09_TAB
* | [<-->] CT_CURRENCYAMOUNT              TYPE        BAPIACCR09_TAB
* | [<-->] CT_EXTENSION1                  TYPE        BAPIACEXTC_TAB
* | [<-()] RV_BOOL                        TYPE        BOOLEAN
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD apportionment_check.

    MESSAGE i044 INTO DATA(dummy).                          "#EC NEEDED
    zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
    zcl_ptool_helper=>bal_db_save( ).

*** BrunoCappellini-23.04.2024 {
    LOOP AT ct_accountgl ASSIGNING FIELD-SYMBOL(<fs_accountgl>) WHERE alloc_nmbr IS INITIAL.
      <fs_accountgl>-alloc_nmbr = 'R'.
    ENDLOOP.
*** } BrunoCappellini-23.04.2024

    CALL FUNCTION 'BAPI_ACC_DOCUMENT_CHECK'
      EXPORTING
        documentheader = is_documentheader
      TABLES
        accountgl      = ct_accountgl
        currencyamount = ct_currencyamount
        extension1     = ct_extension1
        return         = mt_return.

    LOG-POINT ID zcr047 FIELDS mt_return .

    LOOP AT mt_return ASSIGNING FIELD-SYMBOL(<return>).
      MESSAGE ID <return>-id
         TYPE <return>-type
       NUMBER <return>-number
         WITH <return>-message_v1 <return>-message_v2 <return>-message_v3 <return>-message_v4
         INTO dummy.
      zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
    ENDLOOP.
    zcl_ptool_helper=>bal_db_save( ).

    IF NOT line_exists( mt_return[ type = CONV bapi_mtype( `E` ) ] ).

      DATA(message) = VALUE #( mt_return[ type = CONV bapi_mtype( `S` )
                                            id = CONV symsgid( 'RW' )
                                        number = CONV symsgno( '605' ) ] OPTIONAL ).
      IF message IS NOT INITIAL.
        rv_bool = abap_true.
      ENDIF.

      IF _mass IS NOT INITIAL AND rv_bool = abap_true.
        CALL FUNCTION 'BAPI_TRANSACTION_COMMIT'
          EXPORTING
            wait = abap_true.
      ENDIF.
    ELSE.
      _has_errors = abap_true.
    ENDIF.

  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method ZCL_RATEIO_PEP_NEW->APPORTIONMENT_CALCULATE_RECLAS
* +-------------------------------------------------------------------------------------------------+
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD apportionment_calculate_reclas.

    DATA:
      lt_accountgl      TYPE bapiacgl09_tab,
      lt_currencyamount TYPE bapiaccr09_tab,
      lt_extension1     TYPE bapiacextc_tab,
      break             TYPE boolean VALUE abap_true,
      error             TYPE boolean VALUE abap_false.

    MESSAGE i041 INTO DATA(dummy).                          "#EC NEEDED
    zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
    zcl_ptool_helper=>bal_db_save( ).

    LOOP AT mt_rv_documents ASSIGNING FIELD-SYMBOL(<key>)
                          WHERE ( ps_psp_pnr IS NOT INITIAL AND racct NOT IN r_racct_ze[] )
                          GROUP BY ( rldnr = <key>-rldnr
                                    rbukrs = <key>-rbukrs
                                     gjahr = <key>-gjahr
                                     belnr = <key>-belnr )
                          ASSIGNING FIELD-SYMBOL(<group>).

      DATA(ls_acdoca_key) = VALUE ty_s_acdoca_key(
                            rldnr = <group>-rldnr
                           rbukrs = <group>-rbukrs
                            gjahr = <group>-gjahr
                            belnr = <group>-belnr ).

      IF line_exists( mt_processed_items[ KEY sgtxt sgtxt = CONV #( ls_acdoca_key ) ] ) OR
         line_exists( mt_processed_items[ KEY bktxt bktxt = CONV #( ls_acdoca_key ) ] ).
        CONTINUE.
      ENDIF.

      MESSAGE i042 WITH <group>-rldnr <group>-rbukrs <group>-gjahr <group>-belnr INTO dummy.
      zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
      zcl_ptool_helper=>bal_db_save( ).

      break = abap_true.

      LOOP AT GROUP <group> ASSIGNING FIELD-SYMBOL(<member>).

        IF _posting_date IS INITIAL.
          _posting_date = <member>-budat.
        ENDIF.

        DATA(lv_doc_type) = 'ZE'.
        DATA(ls_log) = zcl_ptool_helper=>bal_db_read(  i_log_handle = _handle ).
        ls_log-extnumber = <member>-posid_edit .
        zcl_ptool_helper=>bal_log_hdr_change( i_log_handle = _handle i_s_log = ls_log  ).

        IF break = abap_true.
          IF <member>-vbeln IS NOT INITIAL AND <member>-fkart = 'S1'.
            error = abap_true.
            IF <member>-ze_bukrs IS NOT INITIAL AND
               <member>-ze_belnr IS NOT INITIAL AND
               <member>-ze_gjahr IS NOT INITIAL.
              lv_doc_type = 'EE'.
              MESSAGE i100 WITH <member>-rldnr <member>-rbukrs <member>-gjahr <member>-belnr INTO dummy.
              IF apportionment_reversal_ze_zi( VALUE #( awtyp = <member>-ze_awtyp
                                                        awkey = <member>-ze_awkey
                                                        awsys = <member>-ze_awsys
                                                        bukrs = <member>-ze_bukrs
                                                        glvor = <member>-ze_glvor ) ).
              ENDIF.
            ELSE.
              CLEAR lv_doc_type.
              MESSAGE e098 WITH <member>-rldnr <member>-rbukrs <member>-gjahr <member>-rv_belnr INTO dummy.
            ENDIF.
          ELSE.
            MESSAGE i042 WITH <member>-rldnr <member>-rbukrs <member>-gjahr <member>-belnr INTO dummy.
          ENDIF.
          zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
          zcl_ptool_helper=>bal_db_save( ).
          break = abap_false.
        ENDIF.

        IF line_exists( mt_processed_items[ KEY xblnr rbukrs = <member>-rbukrs xblnr = <member>-belnr ] ) OR error = abap_true.
          CONTINUE.
        ENDIF.

        IF error IS INITIAL.

          DATA(ls_documentheader) = VALUE bapiache09(
                         username = sy-uname
                       header_txt = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
                        comp_code = <member>-rbukrs
*                         doc_date = sy-datum
*                       pstng_date = sy-datum
                         doc_date = <member>-bldat
                       pstng_date = _posting_date
*                        fisc_year = <member>-gjahr
                       ref_doc_no = <member>-belnr ).

          DATA(ls_1st_accountgl) = VALUE bapiacgl09(
                           itemno_acc = CONV posnr_acc( |{ lines( lt_accountgl ) + 1 }| )
                           gl_account = <member>-racct
                            item_text = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
                            ref_key_3 = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
*                           doc_type = 'SA'
**                           doc_type = 'ZE'
                             doc_type = lv_doc_type
                            comp_code = <group>-rbukrs
*                            fisc_year = <group>-gjahr
*                           pstng_date = sy-datum
                           pstng_date = _posting_date
                          wbs_element = <member>-posid_edit ).
          INSERT ls_1st_accountgl INTO TABLE lt_accountgl.

          DATA(ls_1st_currencyamount) = VALUE bapiaccr09(
                           itemno_acc = CONV posnr_acc( |{ lines( lt_currencyamount ) + 1 }| )
                            curr_type = '00'
                             currency = <member>-rwcur
                         currency_iso = <member>-rwcur
                           amt_doccur = - <member>-wsl ).
          INSERT ls_1st_currencyamount INTO TABLE lt_currencyamount REFERENCE INTO DATA(lr_1st_currencyamount).

          DATA(acumulado) = CONV fins_vhcur12( 0 ).
*        DATA(saldo) = lr_1st_currencyamount->amt_doccur.
          DATA(lt_filter) = FILTER #( mt_wbs_elements WHERE psphi = <member>-ps_prj_pnr ).
*        LOOP AT mt_wbs_elements ASSIGNING FIELD-SYMBOL(<filter>) WHERE psphi = <member>-ps_prj_pnr .
          LOOP AT lt_filter ASSIGNING FIELD-SYMBOL(<filter>).
            DATA(ls_accountgl) = VALUE bapiacgl09(
                           itemno_acc = CONV posnr_acc( |{ lines( lt_accountgl ) + 1 }| )
                           gl_account = <member>-racct
                            item_text = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
                            ref_key_3 = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
*                           doc_type = 'SA'
**                           doc_type = 'ZE'
                             doc_type = lv_doc_type
                            comp_code = <group>-rbukrs
*                            fisc_year = <group>-gjahr
*                           pstng_date = sy-datum
                           pstng_date = _posting_date
                          wbs_element = <filter>-posid_edit ).
            INSERT ls_accountgl INTO TABLE lt_accountgl.

            DATA(ls_currencyamount) = VALUE bapiaccr09(
                                 itemno_acc = CONV posnr_acc( |{ lines( lt_currencyamount ) + 1 }| )
                                  curr_type = '00'
                                   currency = <member>-rwcur
                               currency_iso = <member>-rwcur
                                 amt_doccur = CONV fins_vhcur12( ( <member>-wsl * <filter>-usr07 ) / 10 ) ).
*          ADD ls_currencyamount-amt_doccur TO saldo.
            AT LAST.
              IF ( lr_1st_currencyamount->amt_doccur -  acumulado ) > 0.
                ls_currencyamount-amt_doccur = ( acumulado - lr_1st_currencyamount->amt_doccur ).
              ELSE.
                ls_currencyamount-amt_doccur = - ( lr_1st_currencyamount->amt_doccur - acumulado ).
              ENDIF.
            ENDAT.

            SUBTRACT ls_currencyamount-amt_doccur FROM acumulado.
            INSERT ls_currencyamount INTO TABLE lt_currencyamount.

            APPEND VALUE bapiacextc( field1 = 'ZZ1_RATEIO_PEP_JEI' field2 = CONV posnr_acc( |{ lines( lt_extension1 ) + 1 }| ) field3 = ( <filter>-usr07 / 10 ) ) TO lt_extension1.

          ENDLOOP.

        ENDIF.

      ENDLOOP.

      LOG-POINT ID zcr047 FIELDS <group> lt_accountgl lt_currencyamount.

      LOOP AT lt_accountgl ASSIGNING FIELD-SYMBOL(<accountgl_log>).
        DATA(ls_currencyamount_log) = VALUE #( lt_currencyamount[ itemno_acc = <accountgl_log>-itemno_acc ] OPTIONAL ).
        MESSAGE i043 WITH <accountgl_log>-wbs_element
                          <accountgl_log>-gl_account
                          ls_currencyamount_log-amt_doccur
           INTO dummy.
        zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
        zcl_ptool_helper=>bal_db_save( ).
      ENDLOOP.

      IF error = abap_false.
        IF lt_accountgl[] IS NOT INITIAL AND lt_currencyamount[] IS NOT INITIAL.
          IF _testrun IS INITIAL.
            IF apportionment_post(
               EXPORTING
                 is_documentheader = ls_documentheader
               CHANGING
                 ct_accountgl      = lt_accountgl
                 ct_currencyamount = lt_currencyamount
                 ct_extension1     = lt_extension1 ).

              GET PARAMETER ID 'BLN' FIELD <member>-ze_belnr.
              GET PARAMETER ID 'BUK' FIELD <member>-ze_bukrs.
              GET PARAMETER ID 'GJR' FIELD <member>-ze_gjahr.
              MODIFY mt_rv_documents
                FROM <member>
                TRANSPORTING ze_belnr ze_gjahr ze_bukrs
                WHERE rldnr = <group>-rldnr
                  AND rbukrs = <group>-rbukrs
                  AND gjahr = <group>-gjahr
                  AND belnr = <group>-belnr.
            ENDIF.
          ELSE.
            IF apportionment_check(
              EXPORTING
                is_documentheader = ls_documentheader
              CHANGING
                ct_accountgl      = lt_accountgl
                ct_currencyamount = lt_currencyamount
                ct_extension1     = lt_extension1 ).
            ENDIF.
          ENDIF.
        ENDIF.
      ENDIF.

      CLEAR:
        ls_documentheader,
        lt_accountgl,
        lt_currencyamount,
        lt_extension1.

    ENDLOOP.

  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method ZCL_RATEIO_PEP_NEW->APPORTIONMENT_CALCULATE_DEB
* +-------------------------------------------------------------------------------------------------+
* | [<-->] CT_DOCUMENTS                   TYPE        ZTTPS_RATEIO_PEP_NEW_SORTED
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD apportionment_calculate_deb.

    DATA:
      lt_accountgl      TYPE bapiacgl09_tab,
      lt_currencyamount TYPE bapiaccr09_tab,
      lt_extension1     TYPE bapiacextc_tab,
      break             TYPE boolean VALUE abap_true,
      error             TYPE boolean VALUE abap_false.

    zcl_ptool_helper=>bal_db_load( iv_handle = _handle ).

    MESSAGE i041 INTO DATA(dummy).                          "#EC NEEDED
    zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
    zcl_ptool_helper=>bal_db_save( ).

    LOOP AT ct_documents  ASSIGNING FIELD-SYMBOL(<key>)
                          WHERE ( racct <> '2990100001' )
                          GROUP BY ( rldnr = <key>-rldnr
                                    rbukrs = <key>-rbukrs
                                     gjahr = <key>-gjahr
                                     belnr = <key>-belnr )
                          ASSIGNING FIELD-SYMBOL(<group>).


      DATA(ls_acdoca_key) = VALUE ty_s_acdoca_key( rldnr = <group>-rldnr
                                                  rbukrs = <group>-rbukrs
                                                   gjahr = <group>-gjahr
                                                   belnr = <group>-belnr ).

      MESSAGE i042 WITH <group>-rldnr <group>-rbukrs <group>-gjahr <group>-belnr INTO dummy.
      zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
      zcl_ptool_helper=>bal_db_save( ).

      DATA(_tabix) = 0.

      LOOP AT GROUP <group> ASSIGNING FIELD-SYMBOL(<member>).

        ADD 1 TO _tabix.

        IF _posting_date IS INITIAL.
          _posting_date = <member>-budat.
        ENDIF.

        DATA(ls_log) = zcl_ptool_helper=>bal_db_read(  i_log_handle = _handle ).
        IF <member>-posid_edit IS NOT INITIAL.
          ls_log-extnumber = <member>-posid_edit .
          zcl_ptool_helper=>bal_log_hdr_change( i_log_handle = _handle i_s_log = ls_log  ).
        ENDIF.

        "ESTORNO
        IF break = abap_true.
          LOG-POINT ID zcr047 FIELDS break.
          IF <member>-vbeln IS NOT INITIAL AND ( <member>-fkart = 'S1' OR <member>-fkart = 'S2' ).
            error = abap_true.
            LOG-POINT ID zcr047 FIELDS <member> error.
            IF <member>-zi_bukrs IS NOT INITIAL AND
               <member>-zi_belnr IS NOT INITIAL AND
               <member>-zi_gjahr IS NOT INITIAL.
*** BrunoCappellini-25.04.2024 {
              TRY.
                  DATA(lwa_ct_documents) = ct_documents[ 1 ] .
                CATCH cx_sy_itab_line_not_found.
              ENDTRY.
*** } BrunoCappellini-25.04.2024
              IF apportionment_reversal_ze_zi( VALUE #( awtyp    = <member>-zi_awtyp
                                                        awkey    = <member>-zi_awkey
                                                        awsys    = <member>-zi_awsys
                                                        bukrs    = <member>-zi_bukrs
                                                        glvor    = <member>-zi_glvor
                                                        ct_bukrs = lwa_ct_documents-rbukrs "BrunoCappellini-25.04.2024
                                                        ct_belnr = lwa_ct_documents-belnr  "BrunoCappellini-25.04.2024
                                                        ct_gjahr = lwa_ct_documents-gjahr  "BrunoCappellini-25.04.2024
                                              ) ).
*                IF _historico IS INITIAL. "BrunoCappellini-26.04.2024
                IF _historico IS INITIAL AND _mass IS INITIAL. "BrunoCappellini-26.04.2024
                  CALL FUNCTION 'ZFM_CHANGE_DOCUMENT'
                    STARTING NEW TASK 'CHANGE_DOC'
                    EXPORTING
                      i_bukrs               = is_document_id-bukrs
                      i_belnr               = is_document_id-belnr
                      i_gjahr               = is_document_id-gjahr
                      i_bktxt               = CONV bkpf-bktxt( |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }| )
                      i_commit              = _historico
                    EXCEPTIONS
                      communication_failure = 1
                      system_failure        = 2.
                  IF sy-subrc <> 0.
                    MESSAGE ID sy-msgid TYPE sy-msgty NUMBER sy-msgno WITH sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4.
                  ENDIF.
                ELSE.
                  change_document( iv_bukrs = is_document_id-bukrs
                                   iv_belnr = is_document_id-belnr
                                   iv_gjahr = is_document_id-gjahr
                                   iv_bktxt = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }| ).
                ENDIF.
              ENDIF.
            ELSE.
              MESSAGE e094 WITH <member>-rldnr <member>-rbukrs <member>-gjahr <member>-rv_belnr INTO dummy.
            ENDIF.
          ENDIF.
          zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
          zcl_ptool_helper=>bal_db_save( ).
          break = abap_false.
        ENDIF.

        "PREENCHENDO A BAPI COM DADOS DO DOCUMENTO ORIGINAL
        DATA(gl_account) = r_racct_destino[ 1 ]-low.
        DATA(wbs_element) = <member>-posid_edit.
        IF error IS INITIAL.

          DATA(ls_documentheader) = VALUE bapiache09( username = sy-uname
                                                    header_txt = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
                                                     comp_code = <member>-rbukrs
                                                      doc_date = <member>-bldat
                                                    pstng_date = _posting_date
                                                      doc_type = 'ZI'
                                                    ref_doc_no = <member>-belnr ).

          DATA(ls_accountgl) = VALUE bapiacgl09( itemno_acc = CONV posnr_acc( |{ lines( lt_accountgl ) + 1 }| )
                                                     gl_account = gl_account
                                                      item_text = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
                                                      ref_key_3 = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
                                                       doc_type = 'ZI'
                                                     alloc_nmbr = COND #( WHEN _tabix = 1 THEN 'C' ELSE abap_false )
                                                     profit_ctr = <member>-prctr
                                                    wbs_element = <member>-posid_edit
                                                      comp_code = <member>-rbukrs
                                                     pstng_date = _posting_date ).

          INSERT ls_accountgl INTO TABLE lt_accountgl.

          DATA(ls_currencyamount) = VALUE bapiaccr09(
                                               itemno_acc = CONV posnr_acc( |{ lines( lt_currencyamount ) + 1 }| )
                                                 currency = <member>-rhcur
                                             currency_iso = <member>-rhcur
                                               amt_doccur = - <member>-hsl ).

          INSERT ls_currencyamount INTO TABLE lt_currencyamount.

          DATA(flow) = VALUE #( mt_flow[ belnr_b1 = <member>-belnr ] OPTIONAL ).

          APPEND VALUE bapiacextc( field1 = CONV posnr_acc( |{ lines( lt_extension1 ) + 1 }| )
                                   field2 = 'ZZ1_RATEIO_PEP_JEI'
                                   field3 = CONV #( COND #( WHEN _tabix = 1 THEN 0 ELSE COND #( WHEN flow IS NOT INITIAL THEN flow-rateio_a ELSE 100 ) ) ) ) TO lt_extension1.

*** BrunoCappellini-23.04.2024 {
          READ TABLE lt_accountgl
           ASSIGNING FIELD-SYMBOL(<ln_accountgl>)
            WITH KEY itemno_acc = ls_accountgl-itemno_acc
                     gl_account = ls_accountgl-gl_account
                     item_text  = ls_accountgl-item_text
                     BINARY SEARCH.

          IF <ln_accountgl> IS ASSIGNED
         AND sy-subrc IS INITIAL.

            READ TABLE lt_extension1
             ASSIGNING FIELD-SYMBOL(<fs_ext>)
              WITH KEY field1 = ls_accountgl-itemno_acc
                       BINARY SEARCH.

            IF <fs_ext> IS ASSIGNED
           AND sy-subrc IS INITIAL.

              DATA(amt_aux) = CONV fins_vhcur12( <fs_ext>-field3 ).

              IF <ln_accountgl>-doc_type = 'ZI'.
                IF amt_aux GT 0.
                  <ln_accountgl>-alloc_nmbr = 'R1'.
                ENDIF.
              ENDIF.

            ENDIF.
          ENDIF.
*** } BrunoCappellini-23.04.2024

        ENDIF.

      ENDLOOP.

      LOG-POINT ID zcr047 FIELDS <group> lt_accountgl lt_currencyamount lt_extension1.

      LOOP AT lt_accountgl ASSIGNING FIELD-SYMBOL(<accountgl_log>).
        DATA(ls_currencyamount_log) = VALUE #( lt_currencyamount[ itemno_acc = <accountgl_log>-itemno_acc ] OPTIONAL ).
        MESSAGE i043 WITH <accountgl_log>-wbs_element
                          <accountgl_log>-gl_account
                          ls_currencyamount_log-amt_doccur
                     INTO dummy.
        zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
        zcl_ptool_helper=>bal_db_save( ).
      ENDLOOP.

      IF error = abap_false.

        IF lt_accountgl[] IS NOT INITIAL AND lt_currencyamount[] IS NOT INITIAL.

          DATA(documents) = FILTER #( ct_documents WHERE rldnr = <group>-rldnr AND belnr = <group>-belnr AND rbukrs = <group>-rbukrs AND gjahr = <group>-gjahr ).
          DATA(linhas_cliente) = FILTER #( documents USING KEY koart WHERE koart = 'D' ).
          SELECT *
            FROM @ct_documents AS i
           WHERE rldnr = @<group>-rldnr
             AND belnr = @<group>-belnr
             AND rbukrs = @<group>-rbukrs
             AND gjahr = @<group>-gjahr
             AND racct IN @r_racct_zi[]
            INTO TABLE @DATA(profit_centers).
          IF sy-subrc = 0.
            DATA(profit_center) = VALUE #( profit_centers[ 1 ]-prctr OPTIONAL ).
          ENDIF.
          IF linhas_cliente[] IS NOT INITIAL.

            DATA(linha_cliente) = linhas_cliente[ 1 ].

            INSERT VALUE bapiacgl09( itemno_acc = CONV posnr_acc( |{ lines( lt_accountgl ) + 1 }| )
                                     gl_account = r_racct_destino[ 1 ]-low
                                      item_text = |{ linha_cliente-belnr }{ linha_cliente-rbukrs }{ linha_cliente-gjahr }|
                                      ref_key_3 = |{ linha_cliente-belnr }{ linha_cliente-rbukrs }{ linha_cliente-gjahr }|
                                       doc_type = 'ZI'
                                     alloc_nmbr = 'R'
                                    wbs_element = wbs_element
                                      comp_code = linha_cliente-rbukrs
                                     pstng_date = _posting_date ) INTO TABLE lt_accountgl.
            INSERT VALUE bapiaccr09( LET _amt_doccur = CONV fins_vhcur12( abs( linha_cliente-hsl ) )
                                      IN
                                          itemno_acc = CONV posnr_acc( |{ lines( lt_currencyamount ) + 1 }| )
                                            currency = linha_cliente-rhcur
                                        currency_iso = linha_cliente-rhcur
                                          amt_doccur = COND #( WHEN <member>-fkart = 'ZDRB' THEN - _amt_doccur ELSE _amt_doccur ) )
                                    INTO TABLE lt_currencyamount.

            INSERT VALUE bapiacgl09( itemno_acc = CONV posnr_acc( |{ lines( lt_accountgl ) + 1 }| )
                                     gl_account = r_racct_destino[ 1 ]-low
                                      item_text = |{ linha_cliente-belnr }{ linha_cliente-rbukrs }{ linha_cliente-gjahr }|
                                      ref_key_3 = |{ linha_cliente-belnr }{ linha_cliente-rbukrs }{ linha_cliente-gjahr }|
                                       doc_type = 'ZI'
                                     alloc_nmbr = 'C' "abap_true
                                    wbs_element = wbs_element
                                     profit_ctr = profit_center
                                      comp_code = linha_cliente-rbukrs
                                     pstng_date = _posting_date ) INTO TABLE lt_accountgl.
            INSERT VALUE bapiaccr09( LET _amt_doccur = CONV fins_vhcur12( abs( linha_cliente-hsl ) )
                                      IN
                                          itemno_acc = CONV posnr_acc( |{ lines( lt_currencyamount ) + 1 }| )
                                            currency = linha_cliente-rhcur
                                        currency_iso = linha_cliente-rhcur
                                          amt_doccur = COND #( WHEN <member>-fkart = 'ZDRB' THEN _amt_doccur ELSE - _amt_doccur ) )
                                    INTO TABLE lt_currencyamount.
          ENDIF.
          IF _testrun IS INITIAL.
            IF apportionment_post(
              EXPORTING
                is_documentheader = ls_documentheader
              CHANGING
                ct_accountgl      = lt_accountgl
                ct_currencyamount = lt_currencyamount
                ct_extension1     = lt_extension1 ).
              GET PARAMETER ID 'BLN' FIELD <member>-zi_belnr.
              GET PARAMETER ID 'BUK' FIELD <member>-zi_bukrs.
              GET PARAMETER ID 'GJR' FIELD <member>-zi_gjahr.
              MODIFY mt_rv_documents
                FROM <member>
                TRANSPORTING zi_belnr zi_gjahr zi_bukrs
                WHERE rldnr = <member>-rldnr
                  AND rbukrs = <member>-rbukrs
                  AND gjahr = <member>-gjahr
                  AND belnr = <member>-belnr.
            ENDIF.
          ELSE.
            IF apportionment_check(
              EXPORTING
                is_documentheader = ls_documentheader
              CHANGING
                ct_accountgl      = lt_accountgl
                ct_currencyamount = lt_currencyamount
                ct_extension1     = lt_extension1 ).
              GET PARAMETER ID 'BLN' FIELD <member>-zi_belnr.
              GET PARAMETER ID 'BUK' FIELD <member>-zi_bukrs.
              GET PARAMETER ID 'GJR' FIELD <member>-zi_gjahr.
              MODIFY mt_rv_documents
                FROM <member>
                TRANSPORTING zi_belnr zi_gjahr zi_bukrs
                WHERE rldnr = <member>-rldnr
                  AND rbukrs = <member>-rbukrs
                  AND gjahr = <member>-gjahr
                  AND belnr = <member>-belnr.
            ENDIF.

          ENDIF.
        ENDIF.
      ENDIF.

      CLEAR:
        ls_documentheader,
        lt_accountgl,
        lt_currencyamount,
        lt_extension1.

    ENDLOOP.

  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method ZCL_RATEIO_PEP_NEW->APPORTIONMENT_CALCULATE_CR_FAT
* +-------------------------------------------------------------------------------------------------+
* | [<-->] CT_DOCUMENTS                   TYPE        ZTTPS_RATEIO_PEP_NEW_SORTED
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD apportionment_calculate_cr_fat.

    "CR79 lançamento de documento atribuído o número do PEP ao cliente
    DATA:
      lt_accountgl      TYPE bapiacgl09_tab,
      lt_currencyamount TYPE bapiaccr09_tab,
      lt_extension1     TYPE bapiacextc_tab,
      break             TYPE boolean VALUE abap_true,
      error             TYPE boolean VALUE abap_false,
      posnr             TYPE posnr_acc,
      estornos          TYPE RANGE OF fkart.

    zcl_ptool_helper=>bal_db_load( iv_handle = _handle ).

    MESSAGE i041 INTO DATA(dummy).                          "#EC NEEDED
    zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
    zcl_ptool_helper=>bal_db_save( ).

    estornos = VALUE #( sign = 'I' option = 'EQ' ( low = 'S1' ) ( low = 'S2' ) ) .
    posnr = 0.

    LOOP AT ct_documents  ASSIGNING FIELD-SYMBOL(<key>)
                          WHERE ( ps_psp_pnr IS NOT INITIAL AND racct IN r_racct_zi[] )
                          GROUP BY ( rldnr = <key>-rldnr
                                    rbukrs = <key>-rbukrs
                                     gjahr = <key>-gjahr
                                     belnr = <key>-belnr )
                          ASSIGNING FIELD-SYMBOL(<group>).


      DATA(ls_acdoca_key) = VALUE ty_s_acdoca_key( rldnr = <group>-rldnr
                                                  rbukrs = <group>-rbukrs
                                                   gjahr = <group>-gjahr
                                                   belnr = <group>-belnr ).

      MESSAGE i042 WITH <group>-rldnr <group>-rbukrs <group>-gjahr <group>-belnr INTO dummy.
      zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
      zcl_ptool_helper=>bal_db_save( ).

      LOOP AT GROUP <group> ASSIGNING FIELD-SYMBOL(<member>).

*        IF ( <member>-fkart = 'S1' AND <member>-bschl = '50' ) OR
*           ( <member>-fkart <> 'S1' AND <member>-bschl = '40' ).
*          IF <member>-racct IN r_racct_ze[] AND
*             <member>-rwcur <> 'BRL'.
*            CONTINUE.
*          ENDIF.
*        ENDIF.
        IF ( <member>-fkart IN estornos[] AND <member>-bschl = '50' ) OR
           ( <member>-fkart NOT IN estornos[] AND <member>-bschl = '40' ).
          IF <member>-racct IN r_racct_ze[] AND
             <member>-rwcur <> 'BRL'.
            CONTINUE.
          ENDIF.
        ENDIF.

        IF _historico = abap_false AND _noze = abap_false.
          CHECK <member>-ze_belnr IS NOT INITIAL.
        ENDIF.

        IF _posting_date IS INITIAL.
          _posting_date = <member>-budat.
        ENDIF.

        DATA(ls_log) = zcl_ptool_helper=>bal_db_read(  i_log_handle = _handle ).
        IF <member>-posid_edit IS NOT INITIAL.
          ls_log-extnumber = <member>-posid_edit .
          zcl_ptool_helper=>bal_log_hdr_change( i_log_handle = _handle i_s_log = ls_log  ).
        ENDIF.

        IF break = abap_true.
          LOG-POINT ID zcr047 FIELDS break.
*          IF <member>-vbeln IS NOT INITIAL AND <member>-fkart = 'S1'.
          IF <member>-vbeln IS NOT INITIAL AND <member>-fkart IN estornos[].
            error = abap_true.
            LOG-POINT ID zcr047 FIELDS <member> error.
            IF <member>-zi_bukrs IS NOT INITIAL AND
               <member>-zi_belnr IS NOT INITIAL AND
               <member>-zi_gjahr IS NOT INITIAL.
*** BrunoCappellini-25.04.2024 {
*              lv_doc_type = 'EI'. ??
              TRY.
                  DATA(lwa_ct_documents) = ct_documents[ 1 ] .
                CATCH cx_sy_itab_line_not_found.
              ENDTRY.
*** } BrunoCappellini-25.04.2024
              IF apportionment_reversal_ze_zi( VALUE #( awtyp = <member>-zi_awtyp
                                                        awkey = <member>-zi_awkey
                                                        awsys = <member>-zi_awsys
                                                        bukrs = <member>-zi_bukrs
                                                        glvor = <member>-zi_glvor
                                                        ct_bukrs = lwa_ct_documents-rbukrs "BrunoCappellini-25.04.2024
                                                        ct_belnr = lwa_ct_documents-belnr  "BrunoCappellini-25.04.2024
                                                        ct_gjahr = lwa_ct_documents-gjahr  "BrunoCappellini-25.04.2024
                                              ) ).
                IF _historico IS INITIAL AND _mass IS INITIAL.
                  CALL FUNCTION 'ZFM_CHANGE_DOCUMENT'
                    STARTING NEW TASK 'CHANGE_DOC'
                    EXPORTING
                      i_bukrs               = is_document_id-bukrs
                      i_belnr               = is_document_id-belnr
                      i_gjahr               = is_document_id-gjahr
                      i_bktxt               = CONV bkpf-bktxt( |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }| )
                      i_commit              = _historico
                    EXCEPTIONS
                      communication_failure = 1
                      system_failure        = 2.
                  IF sy-subrc <> 0.
                    MESSAGE ID sy-msgid TYPE sy-msgty NUMBER sy-msgno WITH sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4.
                  ENDIF.
                ELSE.
                  change_document( iv_bukrs = is_document_id-bukrs
                                   iv_belnr = is_document_id-belnr
                                   iv_gjahr = is_document_id-gjahr
                                   iv_bktxt = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }| ).
                ENDIF.
              ENDIF.
            ELSE.
              MESSAGE e094 WITH <member>-rldnr <member>-rbukrs <member>-gjahr <member>-rv_belnr INTO dummy.
            ENDIF.
          ENDIF.
          zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
          zcl_ptool_helper=>bal_db_save( ).
          break = abap_false.
        ENDIF.

        IF error IS INITIAL.
          DATA(ls_flow) = VALUE #( mt_flow[ belnr_k = <member>-belnr ] OPTIONAL ).
          DATA(ls_documentheader) = VALUE bapiache09( username = sy-uname
                                                    header_txt = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
                                                     comp_code = <member>-rbukrs
                                                      doc_date = <member>-bldat
                                                    pstng_date = _posting_date
                                                      doc_type = 'ZI'
*                                                     fisc_year = <member>-gjahr
                                                    ref_doc_no = <member>-belnr ).
          DATA(gl_account) = r_racct_destino[ 1 ]-low.
          IF <member>-fkart IN r_fkart_exc[].
            IF <member>-rbukrs IN r_bukrs_exc[] .
              IF _0005 = abap_true.
                IF <member>-racct IN r_racct_zi[] AND <member>-bschl = '40'.
                  gl_account = '3102010100'.
                  CONTINUE.
                ENDIF.
              ENDIF.
            ENDIF.
          ENDIF.

          posnr += 1.

          DATA(ls_1st_accountgl) = VALUE bapiacgl09( itemno_acc = CONV posnr_acc( |{ posnr }| )
                                                     gl_account = gl_account
                                                      item_text = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
                                                      ref_key_3 = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
                                                       doc_type = 'ZI'
                                                     alloc_nmbr = 'C' "abap_true
*                                                    wbs_element = <member>-posid_edit
                                                     profit_ctr = COND #( WHEN <member>-racct IN r_racct_zi[] THEN <member>-prctr ELSE abap_false )
                                                      comp_code = <member>-rbukrs
*                                                      fisc_year = <member>-gjahr
                                                     pstng_date = _posting_date ).

          INSERT ls_1st_accountgl INTO TABLE lt_accountgl.

          DATA(ls_1st_currencyamount) = VALUE bapiaccr09(
                                          LET _amt_doccur = CONV fins_vhcur12( abs( <member>-hsl ) )
                                           IN
                                               itemno_acc = CONV posnr_acc( |{ posnr }| )
                                                 currency = <member>-rhcur
                                             currency_iso = <member>-rhcur
                                               amt_doccur = COND #( WHEN <member>-fkart IN r_fkart_exc[] THEN - _amt_doccur ELSE _amt_doccur ) ).
*                                                exch_rate = <member>-kursf ).
          INSERT ls_1st_currencyamount INTO TABLE lt_currencyamount REFERENCE INTO DATA(lr_1st_currencyamount).

          APPEND VALUE bapiacextc( field1 = CONV posnr_acc( |{ posnr }| )
                                   field2 = 'ZZ1_RATEIO_PEP_JEI'
                                   field3 = CONV #( 0 ) ) TO lt_extension1.

          APPEND VALUE bapiacextc( field1 = CONV posnr_acc( |{ posnr }| )
                                   field2 = 'XREF1_HD'
                                   field3 = ls_flow-belnr_b1 ) TO lt_extension1.

          DATA(acumulado) = CONV fins_vhcur12( 0 ).
          IF <member>-fkart IN r_fkart_flow[].
            DATA(lt_filter_flow) = FILTER #( mt_flow USING KEY psphi WHERE belnr_k = <member>-belnr AND psphi = <member>-ps_prj_pnr ).
            LOOP AT lt_filter_flow ASSIGNING FIELD-SYMBOL(<filter_flow>)
                                   WHERE racct_a = '2105010050'
                                     AND rateio_a > 0.

              posnr += 1.

              DATA(ls_accountgl) = VALUE bapiacgl09(
                      itemno_acc = CONV posnr_acc( |{ posnr }| )
                      gl_account = gl_account
                       item_text = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
                       ref_key_3 = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
                        doc_type = 'ZI'
                       comp_code = <group>-rbukrs
*                     fisc_year = <group>-gjahr
                      pstng_date = _posting_date
                     wbs_element = <filter_flow>-posid_edit ).
              INSERT ls_accountgl INTO TABLE lt_accountgl.

              DATA(ls_currencyamount) = VALUE bapiaccr09(
                                          LET _amt_doccur = CONV fins_vhcur12( abs( ( <member>-hsl * <filter_flow>-rateio_a ) / 100 ) )
                                           IN
                                               itemno_acc = CONV posnr_acc( |{ posnr }| )
                                                 currency = <member>-rhcur
                                             currency_iso = <member>-rhcur
                                               amt_doccur = COND #( WHEN <member>-fkart IN r_fkart_exc[] THEN _amt_doccur ELSE - _amt_doccur ) ).
*                                              exch_rate = <member>-kursf  ).
              AT LAST.
                IF ( lr_1st_currencyamount->amt_doccur -  acumulado ) > 0.
                  ls_currencyamount-amt_doccur = ( acumulado - lr_1st_currencyamount->amt_doccur ).
                ELSE.
                  ls_currencyamount-amt_doccur = - ( lr_1st_currencyamount->amt_doccur - acumulado ).
                ENDIF.
              ENDAT.

              SUBTRACT ls_currencyamount-amt_doccur FROM acumulado.
              INSERT ls_currencyamount INTO TABLE lt_currencyamount.

*** BrunoCappellini-23.04.2024 {
              READ TABLE lt_accountgl
               ASSIGNING FIELD-SYMBOL(<ln_accountgl>)
                WITH KEY itemno_acc = ls_accountgl-itemno_acc
                         gl_account = ls_accountgl-gl_account
                         item_text  = ls_accountgl-item_text
                         BINARY SEARCH.

              IF ( <ln_accountgl> IS ASSIGNED ).

                IF ( <ln_accountgl>-alloc_nmbr IS INITIAL ).
                  CASE <ln_accountgl>-doc_type.
                    WHEN 'ZE'.
                      IF <filter_flow>-rateio_a EQ 0 .
                        <ln_accountgl>-alloc_nmbr = 'R'.
                      ELSEIF <filter_flow>-rateio_a GT 0 .
                        <ln_accountgl>-alloc_nmbr = 'R1'.
                      ENDIF.
                    WHEN 'ZI'.
                      IF <filter_flow>-rateio_a GT 0.
                        <ln_accountgl>-alloc_nmbr = 'R1'.
                      ENDIF.
                    WHEN OTHERS.
                  ENDCASE.
                ENDIF.

              ENDIF.
*** } BrunoCappellini-23.04.2024

              APPEND VALUE bapiacextc( field1 = CONV posnr_acc( |{ posnr }| )
                                       field2 = 'ZZ1_RATEIO_PEP_JEI'
                                       field3 = |{ <filter_flow>-rateio_a DECIMALS = 2 }| ) TO lt_extension1.

              APPEND VALUE bapiacextc( field1 = CONV posnr_acc( |{ posnr }| )
                                       field2 = 'XREF1_HD'
                                       field3 = ls_flow-belnr_b1 ) TO lt_extension1.

            ENDLOOP.

          ELSE.
            DATA(lt_filter) = FILTER #( mt_wbs_elements WHERE psphi = <member>-ps_prj_pnr  ).
            DATA(wbs_element) = <member>-posid_edit.

            LOOP AT lt_filter ASSIGNING FIELD-SYMBOL(<filter>).

              posnr += 1.

              ls_accountgl = VALUE bapiacgl09( itemno_acc = CONV posnr_acc( |{ posnr }| )
                                                     gl_account = gl_account
                                                      item_text = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
                                                      ref_key_3 = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
                                                       doc_type = 'ZI'
                                                      comp_code = <member>-rbukrs
*                                                    fisc_year = <member>-gjahr
                                                     pstng_date = _posting_date
                                                    wbs_element = <filter>-posid_edit ).
              INSERT ls_accountgl INTO TABLE lt_accountgl.

              ls_currencyamount = VALUE bapiaccr09(
                                          LET _amt_doccur = CONV fins_vhcur12( abs( ( <member>-hsl * <filter>-usr07 ) / 10 ) )
                                           IN
                                               itemno_acc = CONV posnr_acc( |{ posnr }| )
                                                 currency = <member>-rhcur
                                             currency_iso = <member>-rhcur
                                               amt_doccur = COND #( WHEN <member>-fkart IN r_fkart_exc[] THEN _amt_doccur ELSE - _amt_doccur ) ).
*                                              exch_rate = <member>-kursf  ).
              AT LAST.
                IF ( lr_1st_currencyamount->amt_doccur - acumulado ) > 0.
                  ls_currencyamount-amt_doccur = ( acumulado - lr_1st_currencyamount->amt_doccur ).
                ELSE.
                  ls_currencyamount-amt_doccur = - ( lr_1st_currencyamount->amt_doccur - acumulado ).
                ENDIF.
              ENDAT.
              SUBTRACT ls_currencyamount-amt_doccur FROM acumulado.
              INSERT ls_currencyamount INTO TABLE lt_currencyamount.

*** BrunoCappellini-23.04.2024 {
              READ TABLE lt_accountgl
               ASSIGNING <ln_accountgl>
                WITH KEY itemno_acc = ls_accountgl-itemno_acc
                         gl_account = ls_accountgl-gl_account
                         item_text  = ls_accountgl-item_text
                         BINARY SEARCH.

              IF ( <ln_accountgl> IS ASSIGNED ).

                IF ( <ln_accountgl>-alloc_nmbr IS INITIAL ).
                  CASE <ln_accountgl>-doc_type.
                    WHEN 'ZE'.
                      IF <filter>-usr07 EQ 0 .
                        <ln_accountgl>-alloc_nmbr = 'R'.
                      ELSEIF <filter>-usr07 GT 0 .
                        <ln_accountgl>-alloc_nmbr = 'R1'.
                      ENDIF.
                    WHEN 'ZI'.
                      IF <filter>-usr07 GT 0.
                        <ln_accountgl>-alloc_nmbr = 'R1'.
                      ENDIF.
                    WHEN OTHERS.
                  ENDCASE.
                ENDIF.

              ENDIF.
*** } BrunoCappellini-23.04.2024

              APPEND VALUE bapiacextc( field1 = CONV posnr_acc( |{ posnr }| )
                                       field2 = 'ZZ1_RATEIO_PEP_JEI'
                                       field3 = |{ <filter>-usr07 * 10 DECIMALS = 2 }| ) TO lt_extension1.

              APPEND VALUE bapiacextc( field1 = CONV posnr_acc( |{ posnr }| )
                                       field2 = 'XREF1_HD'
                                       field3 = ls_flow-belnr_b1 ) TO lt_extension1.

            ENDLOOP.
          ENDIF.
        ENDIF.

      ENDLOOP.

      LOG-POINT ID zcr047 FIELDS <group> lt_accountgl lt_currencyamount.

      LOOP AT lt_accountgl ASSIGNING FIELD-SYMBOL(<accountgl_log>).
        DATA(ls_currencyamount_log) = VALUE #( lt_currencyamount[ itemno_acc = <accountgl_log>-itemno_acc ] OPTIONAL ).
        MESSAGE i043 WITH <accountgl_log>-wbs_element
                          <accountgl_log>-gl_account
                          ls_currencyamount_log-amt_doccur
                     INTO dummy.
        zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
        zcl_ptool_helper=>bal_db_save( ).
      ENDLOOP.

      IF error = abap_false.
        IF lt_accountgl[] IS NOT INITIAL AND lt_currencyamount[] IS NOT INITIAL.

          DATA(documents) = FILTER #( ct_documents WHERE rldnr = <group>-rldnr AND belnr = <group>-belnr AND rbukrs = <group>-rbukrs AND gjahr = <group>-gjahr ).
          DATA(linhas_cliente) = FILTER #( documents USING KEY koart WHERE koart = 'D' ).
          SELECT *
            FROM @ct_documents AS i
           WHERE rldnr = @<group>-rldnr
             AND belnr = @<group>-belnr
             AND rbukrs = @<group>-rbukrs
             AND gjahr = @<group>-gjahr
             AND racct IN @r_racct_zi[]
            INTO TABLE @DATA(profit_centers).
          IF sy-subrc = 0.
            DATA(profit_center) = VALUE #( profit_centers[ 1 ]-prctr OPTIONAL ).
            DATA(profit_center_ln) = VALUE #( profit_centers[ 1 ] OPTIONAL ).
          ENDIF.
          IF linhas_cliente[] IS NOT INITIAL.

            DATA(linha_cliente) = linhas_cliente[ 1 ].

            INSERT VALUE bapiacgl09( itemno_acc = CONV posnr_acc( |{ lines( lt_accountgl ) + 1 }| )
                                     gl_account = r_racct_destino[ 1 ]-low
                                      item_text = |{ linha_cliente-belnr }{ linha_cliente-rbukrs }{ linha_cliente-gjahr }|
                                      ref_key_3 = |{ linha_cliente-belnr }{ linha_cliente-rbukrs }{ linha_cliente-gjahr }|
                                       doc_type = 'ZI'
                                     alloc_nmbr = 'R'
*                                    wbs_element = wbs_element "*** BrunoCappellini-04.2024
                                    wbs_element = COND #( WHEN wbs_element IS NOT INITIAL THEN wbs_element ELSE profit_center_ln-posid_edit )  "*** BrunoCappellini-04.2024
                                      comp_code = linha_cliente-rbukrs
*                                      fisc_year = linha_cliente-gjahr
                                     pstng_date = _posting_date ) INTO TABLE lt_accountgl.
            INSERT VALUE bapiaccr09( LET _amt_doccur = CONV fins_vhcur12( abs( linha_cliente-hsl ) )
                                      IN  itemno_acc = CONV posnr_acc( |{ lines( lt_currencyamount ) + 1 }| )
                                            currency = linha_cliente-rhcur
                                        currency_iso = linha_cliente-rhcur
                                          amt_doccur = COND #( WHEN <member>-fkart IN r_fkart_exc[] THEN - _amt_doccur ELSE _amt_doccur ) )
                                    INTO TABLE lt_currencyamount.
*                                           exch_rate = linha_cliente-kursf ) INTO TABLE lt_currencyamount.

            INSERT VALUE bapiacgl09( itemno_acc = CONV posnr_acc( |{ lines( lt_accountgl ) + 1 }| )
                                     gl_account = r_racct_destino[ 1 ]-low
                                      item_text = |{ linha_cliente-belnr }{ linha_cliente-rbukrs }{ linha_cliente-gjahr }|
                                      ref_key_3 = |{ linha_cliente-belnr }{ linha_cliente-rbukrs }{ linha_cliente-gjahr }|
                                       doc_type = 'ZI'
                                     alloc_nmbr = 'C' "abap_true
*                                    wbs_element = wbs_element "*** BrunoCappellini-04.2024
                                    wbs_element = COND #( WHEN wbs_element IS NOT INITIAL THEN wbs_element ELSE profit_center_ln-posid_edit )  "*** BrunoCappellini-04.2024
                                     profit_ctr = profit_center
                                      comp_code = linha_cliente-rbukrs
*                                      fisc_year = linha_cliente-gjahr
                                     pstng_date = _posting_date ) INTO TABLE lt_accountgl.
            INSERT VALUE bapiaccr09( LET _amt_doccur = CONV fins_vhcur12( abs( linha_cliente-hsl ) )
                                      IN
                                          itemno_acc = CONV posnr_acc( |{ lines( lt_currencyamount ) + 1 }| )
                                            currency = linha_cliente-rhcur
                                        currency_iso = linha_cliente-rhcur
                                          amt_doccur = COND #( WHEN <member>-fkart IN r_fkart_exc[] THEN _amt_doccur ELSE - _amt_doccur ) )
                                    INTO TABLE lt_currencyamount.
*                                           exch_rate = linha_cliente-kursf ) INTO TABLE lt_currencyamount.

          ENDIF.

          IF _testrun IS INITIAL.
            IF apportionment_post(
              EXPORTING
                is_documentheader = ls_documentheader
              CHANGING
                ct_accountgl      = lt_accountgl
                ct_currencyamount = lt_currencyamount
                ct_extension1     = lt_extension1 ).
              GET PARAMETER ID 'BLN' FIELD <member>-zi_belnr.
              GET PARAMETER ID 'BUK' FIELD <member>-zi_bukrs.
              GET PARAMETER ID 'GJR' FIELD <member>-zi_gjahr.
              MODIFY mt_rv_documents
                FROM <member>
                TRANSPORTING zi_belnr zi_gjahr zi_bukrs
                WHERE rldnr = <member>-rldnr
                  AND rbukrs = <member>-rbukrs
                  AND gjahr = <member>-gjahr
                  AND belnr = <member>-belnr.
*              CALL FUNCTION 'ZFM_CHANGE_DOCUMENT_NEW' IN BACKGROUND UNIT my_unit
*                EXPORTING
*                  i_bukrs   = <member>-zi_bukrs
*                  i_belnr   = <member>-zi_belnr
*                  i_gjahr   = <member>-zi_gjahr
*                  it_accchg = VALUE arberp_t_accchg( ( fdname = 'PRCTR' newval = '' ) )
*                  i_commit  = _historico.
            ENDIF.
          ELSE.
            IF apportionment_check(
              EXPORTING
                is_documentheader = ls_documentheader
              CHANGING
                ct_accountgl      = lt_accountgl
                ct_currencyamount = lt_currencyamount
                ct_extension1     = lt_extension1 ).
              GET PARAMETER ID 'BLN' FIELD <member>-zi_belnr.
              GET PARAMETER ID 'BUK' FIELD <member>-zi_bukrs.
              GET PARAMETER ID 'GJR' FIELD <member>-zi_gjahr.
              MODIFY mt_rv_documents
                FROM <member>
                TRANSPORTING zi_belnr zi_gjahr zi_bukrs
                WHERE rldnr = <member>-rldnr
                  AND rbukrs = <member>-rbukrs
                  AND gjahr = <member>-gjahr
                  AND belnr = <member>-belnr.            ENDIF.

          ENDIF.
        ENDIF.
      ENDIF.

      CLEAR:
        ls_documentheader,
        lt_accountgl,
        lt_currencyamount,
        lt_extension1.

    ENDLOOP.

  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method ZCL_RATEIO_PEP_NEW->APPORTIONMENT_CALCULATE_CR
* +-------------------------------------------------------------------------------------------------+
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD apportionment_calculate_cr.

    DATA:
      lt_accountgl      TYPE bapiacgl09_tab,
      lt_currencyamount TYPE bapiaccr09_tab,
      lt_extension1     TYPE bapiacextc_tab,
      break             TYPE boolean VALUE abap_true.

    MESSAGE i041 INTO DATA(dummy).                          "#EC NEEDED
    zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
    zcl_ptool_helper=>bal_db_save( ).

    LOOP AT mt_dz_documents ASSIGNING FIELD-SYMBOL(<key>)
                            GROUP BY ( rldnr = <key>-rldnr
                                      rbukrs = <key>-rbukrs
                                       gjahr = <key>-gjahr
                                       belnr = <key>-belnr )
                            ASSIGNING FIELD-SYMBOL(<group>).

      DATA(ls_acdoca_key) = VALUE ty_s_acdoca_key( rldnr = <group>-rldnr
                                                  rbukrs = <group>-rbukrs
                                                   gjahr = <group>-gjahr
                                                   belnr = <group>-belnr ).

      IF line_exists( mt_processed_items[ KEY sgtxt sgtxt = CONV #( ls_acdoca_key ) ] ) OR
         line_exists( mt_processed_items[ KEY bktxt bktxt = CONV #( ls_acdoca_key ) ] ).
        CONTINUE.
      ENDIF.

      break = abap_true.

      LOOP AT GROUP <group> ASSIGNING FIELD-SYMBOL(<member>).

        DATA(ls_log) = zcl_ptool_helper=>bal_db_read(  i_log_handle = _handle ).
        ls_log-extnumber = <member>-posid_edit.
        zcl_ptool_helper=>bal_log_hdr_change( i_log_handle = _handle i_s_log = ls_log  ).

        DATA(lv_doc_type) = 'ZJ'.
        IF break = abap_true.
          IF <member>-rateio IS NOT INITIAL AND <member>-estorno IS INITIAL.
            lv_doc_type = 'EJ'.
            MESSAGE i100 WITH <group>-rldnr <group>-rbukrs <group>-gjahr <group>-belnr INTO dummy.
            IF apportionment_reversal_zj( <member> ).
            ENDIF.
            EXIT.
          ELSE.
            MESSAGE i042 WITH <group>-rldnr <group>-rbukrs <group>-gjahr <group>-belnr INTO dummy.
          ENDIF.
          zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
          zcl_ptool_helper=>bal_db_save( ).
          break = abap_false.
        ENDIF.

*      IF line_exists( mt_processed_items[ KEY xblnr xblnr = <member>-belnr ] ).
*        CONTINUE.
*      ENDIF.

        DATA(ls_documentheader) = VALUE bapiache09( username = sy-uname
*                                                header_txt = 'Rateio Recebimento'
                                                  header_txt = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
                                                   comp_code = <member>-rbukrs
                                                    doc_date = sy-datum
                                                  pstng_date = sy-datum
                                                    doc_type = lv_doc_type
*                                                   fisc_year = <member>-gjahr
*                                                ref_doc_no = <member>-belnr ).
                                                  ref_doc_no = <member>-docorigem ).

        DATA(ls_1st_accountgl) = VALUE bapiacgl09( itemno_acc = CONV posnr_acc( |{ lines( lt_accountgl ) + 1 }| )
*                                                 gl_account = gl_conta
                                                   gl_account = r_racct_destino[ 1 ]-low
*                                                  item_text = 'Rateio do Recebimento Contas a Receber'
                                                    item_text = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
                                                     doc_type = lv_doc_type
                                                    comp_code = <group>-rbukrs
*                                                    fisc_year = <group>-gjahr
                                                   pstng_date = sy-datum ).

        INSERT ls_1st_accountgl INTO TABLE lt_accountgl.

        DATA(ls_1st_currencyamount) = VALUE bapiaccr09( itemno_acc = CONV posnr_acc( |{ lines( lt_currencyamount ) + 1 }| )
                                                          currency = <member>-rwcur
                                                        amt_doccur = <member>-wsl    ).

        INSERT ls_1st_currencyamount INTO TABLE lt_currencyamount REFERENCE INTO DATA(lr_1st_currencyamount).

        DATA(acumulado) = CONV fins_vhcur12( 0 ).
        DATA(lt_filter) = FILTER #( mt_wbs_elements WHERE psphi = _pspnr ).
        LOOP AT lt_filter ASSIGNING FIELD-SYMBOL(<filter>).
          DATA(ls_accountgl) = VALUE bapiacgl09( itemno_acc = CONV posnr_acc( |{ lines( lt_accountgl ) + 1 }| )
                                                 gl_account = r_racct_destino[ 1 ]-low
                                                  item_text = 'Rateio do Recebimento Contas a Receber'(002)
                                                   doc_type = lv_doc_type
                                                  comp_code = <group>-rbukrs
*                                                  fisc_year = <group>-gjahr
                                                 pstng_date = sy-datum
                                                wbs_element = <filter>-posid_edit ).

          INSERT ls_accountgl INTO TABLE lt_accountgl.

          DATA(ls_currencyamount) = VALUE bapiaccr09( itemno_acc = CONV posnr_acc( |{ lines( lt_currencyamount ) + 1 }| )
                                                        currency = <member>-rwcur
                                                      amt_doccur = CONV fins_vhcur12( - ( <member>-wsl * <filter>-usr07 ) / 10 ) ).

*          ADD ls_currencyamount-amt_doccur TO saldo.
          AT LAST.
            IF ( lr_1st_currencyamount->amt_doccur -  acumulado ) > 0.
              ls_currencyamount-amt_doccur = ( acumulado - lr_1st_currencyamount->amt_doccur ).
            ELSE.
              ls_currencyamount-amt_doccur = abs( ( lr_1st_currencyamount->amt_doccur - acumulado ) ).
            ENDIF.
          ENDAT.

          SUBTRACT ls_currencyamount-amt_doccur FROM acumulado.
          INSERT ls_currencyamount INTO TABLE lt_currencyamount.

          APPEND VALUE bapiacextc( field1 = 'ZZ1_RATEIO_PEP_JEI' field2 = CONV posnr_acc( |{ lines( lt_extension1 ) + 1 }| ) field3 = ( <filter>-usr07 / 10 ) ) TO lt_extension1.

        ENDLOOP.

      ENDLOOP.

      LOG-POINT ID zcr047 FIELDS <group> lt_accountgl lt_currencyamount.

      LOOP AT lt_accountgl ASSIGNING FIELD-SYMBOL(<accountgl_log>).
        DATA(ls_currencyamount_log) = VALUE #( lt_currencyamount[ itemno_acc = <accountgl_log>-itemno_acc ] OPTIONAL ).
        MESSAGE i043 WITH <accountgl_log>-wbs_element
                          <accountgl_log>-gl_account
                          ls_currencyamount_log-amt_doccur
                     INTO dummy.
        zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
        zcl_ptool_helper=>bal_db_save( ).
      ENDLOOP.

      IF lt_accountgl[] IS NOT INITIAL AND lt_currencyamount[] IS NOT INITIAL.
        IF _testrun IS INITIAL.
          IF apportionment_post(
            EXPORTING
              is_documentheader = ls_documentheader
            CHANGING
              ct_accountgl      = lt_accountgl
              ct_currencyamount = lt_currencyamount
              ct_extension1     = lt_extension1 ).
            change_document( iv_bukrs = <member>-rbukrs
                             iv_belnr = <member>-belnr
                             iv_gjahr = <member>-gjahr
                             iv_bktxt = |{ <member>-docorigem }{ <member>-rbukrs }{ <member>-gjahr }| ).
          ENDIF.
        ELSE.
          apportionment_check(
            EXPORTING
              is_documentheader = ls_documentheader
            CHANGING
              ct_accountgl      = lt_accountgl
              ct_currencyamount = lt_currencyamount
              ct_extension1     = lt_extension1 ).
        ENDIF.
      ENDIF.
      CLEAR: ls_documentheader, lt_accountgl, lt_currencyamount, lt_extension1 .
    ENDLOOP.
  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Public Method ZCL_RATEIO_PEP_NEW->APPORTIONMENT_CALCULATE
* +-------------------------------------------------------------------------------------------------+
* | [<-->] CT_DOCUMENTS                   TYPE        ZTTPS_RATEIO_PEP_NEW_SORTED
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD apportionment_calculate.

    DATA:
      lt_accountgl      TYPE bapiacgl09_tab,
      lt_currencyamount TYPE bapiaccr09_tab,
      lt_extension1     TYPE bapiacextc_tab,
      break             TYPE boolean VALUE abap_true,
      error             TYPE boolean VALUE abap_false,
      posnr             TYPE posnr_acc.

    zcl_ptool_helper=>bal_db_load( iv_handle = _handle ).

    MESSAGE i041 INTO DATA(dummy).                          "#EC NEEDED
    zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
    zcl_ptool_helper=>bal_db_save( ).

    delete_sd_and_reversed( CHANGING ch_documents_tab = ct_documents[] ).
*    CHECK is_deb_reversal( ).

    LOG-POINT ID zcr047 FIELDS ct_documents[] r_racct_ze[].

    LOOP AT ct_documents ASSIGNING FIELD-SYMBOL(<key>)
                         WHERE ( ps_psp_pnr IS NOT INITIAL AND racct NOT IN r_racct_ze[] )
                         GROUP BY ( rldnr = <key>-rldnr
                                   rbukrs = <key>-rbukrs
                                    gjahr = <key>-gjahr
                                    belnr = <key>-belnr )
                         ASSIGNING FIELD-SYMBOL(<group>).

      DATA(ls_acdoca_key) = VALUE ty_s_acdoca_key(
                            rldnr = <group>-rldnr
                           rbukrs = <group>-rbukrs
                            gjahr = <group>-gjahr
                            belnr = <group>-belnr ).

      MESSAGE i042 WITH <group>-rldnr <group>-rbukrs <group>-gjahr <group>-belnr INTO dummy.
      zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
      zcl_ptool_helper=>bal_db_save( ).

      break = abap_true.
      posnr = 0.

      LOOP AT GROUP <group> ASSIGNING FIELD-SYMBOL(<member>).

        IF <member>-racct EQ '2105010001' AND <member>-bschl = '40' AND <member>-rwcur <> 'BRL'.
          CONTINUE.
        ENDIF.

        IF _posting_date IS INITIAL.
          _posting_date = <member>-budat.
        ENDIF.

        DATA(lv_doc_type) = 'ZE'.

        IF _mass IS INITIAL.
          DATA(ls_log) = zcl_ptool_helper=>bal_db_read( i_log_handle = _handle ).
          ls_log-extnumber = <member>-posid_edit .
          zcl_ptool_helper=>bal_log_hdr_change( i_log_handle = _handle i_s_log = ls_log  ).
        ENDIF.

        IF break = abap_true.
          LOG-POINT ID zcr047 FIELDS break.
          IF <member>-vbeln IS NOT INITIAL AND ( <member>-fkart = 'S1' OR <member>-fkart = 'S2' ).
            LOG-POINT ID zcr047 FIELDS <member> error.
            IF <member>-ze_bukrs IS NOT INITIAL AND
               <member>-ze_belnr IS NOT INITIAL AND
               <member>-ze_gjahr IS NOT INITIAL.
              lv_doc_type = 'EE'.
              MESSAGE i100 WITH <member>-rldnr <member>-rbukrs <member>-gjahr <member>-belnr INTO dummy.
              LOG-POINT ID zcr047 FIELDS dummy.
*** BrunoCappellini-25.04.2024 {
              TRY.
                  DATA(lwa_ct_documents) = ct_documents[ 1 ] .
                CATCH cx_sy_itab_line_not_found.
              ENDTRY.
*** } BrunoCappellini-25.04.2024
              IF apportionment_reversal_ze_zi( VALUE #( awtyp    = <member>-ze_awtyp
                                                        awkey    = <member>-ze_awkey
                                                        awsys    = <member>-ze_awsys
                                                        bukrs    = <member>-ze_bukrs
                                                        glvor    = <member>-ze_glvor
                                                        ct_bukrs = lwa_ct_documents-rbukrs "BrunoCappellini-25.04.2024
                                                        ct_belnr = lwa_ct_documents-belnr  "BrunoCappellini-25.04.2024
                                                        ct_gjahr = lwa_ct_documents-gjahr  "BrunoCappellini-25.04.2024
                                              ) ).
                IF _historico IS INITIAL AND _mass IS INITIAL.
                  CALL FUNCTION 'ZFM_CHANGE_DOCUMENT'
                    STARTING NEW TASK 'CHANGE_DOC'
                    EXPORTING
                      i_bukrs               = is_document_id-bukrs
                      i_belnr               = is_document_id-belnr
                      i_gjahr               = is_document_id-gjahr
                      i_bktxt               = CONV bkpf-bktxt( |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }| )
                      i_commit              = _historico
                    EXCEPTIONS
                      communication_failure = 1
                      system_failure        = 2.
                  IF sy-subrc <> 0.
                    MESSAGE ID sy-msgid TYPE sy-msgty NUMBER sy-msgno WITH sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4.
                  ENDIF.
                ELSE.
                  change_document( iv_bukrs = is_document_id-bukrs
                                   iv_belnr = is_document_id-belnr
                                   iv_gjahr = is_document_id-gjahr
                                   iv_bktxt = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }| ).
                ENDIF.
              ENDIF.
              error = abap_true.
            ELSE.
              CLEAR lv_doc_type.
              error = abap_true.
              MESSAGE e098 WITH <member>-rldnr <member>-rbukrs <member>-gjahr <member>-rv_belnr INTO dummy.
            ENDIF.
            IF <member>-zx_belnr IS NOT INITIAL.
              IF apportionment_reversal_ze_zi( VALUE #( awtyp = <member>-zx_awtyp
                                                        awkey = <member>-zx_awkey
                                                        awsys = <member>-zx_awsys
                                                        bukrs = <member>-zx_bukrs
                                                        glvor = <member>-zx_glvor ) ).
                IF _historico IS INITIAL AND _mass IS INITIAL.
                  CALL FUNCTION 'ZFM_CHANGE_DOCUMENT'
                    STARTING NEW TASK 'CHANGE_DOC'
                    EXPORTING
                      i_bukrs               = is_document_id-bukrs
                      i_belnr               = is_document_id-belnr
                      i_gjahr               = is_document_id-gjahr
                      i_bktxt               = CONV bkpf-bktxt( |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }| )
                      i_commit              = _historico
                    EXCEPTIONS
                      communication_failure = 1
                      system_failure        = 2.
                  IF sy-subrc <> 0.
                    MESSAGE ID sy-msgid TYPE sy-msgty NUMBER sy-msgno WITH sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4.
                  ENDIF.
                ELSE.
                  change_document( iv_bukrs = is_document_id-bukrs
                                   iv_belnr = is_document_id-belnr
                                   iv_gjahr = is_document_id-gjahr
                                   iv_bktxt = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }| ).
                ENDIF.
              ENDIF.
            ENDIF.
          ELSE.
            MESSAGE i042 WITH <member>-rldnr <member>-rbukrs <member>-gjahr <member>-belnr INTO dummy.
          ENDIF.
          zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
          zcl_ptool_helper=>bal_db_save( ).
          break = abap_false.
        ENDIF.

        IF error IS INITIAL.

          DATA(ls_flow) = VALUE #( mt_flow[ belnr_k = <member>-belnr ] OPTIONAL ).

          DATA(ls_documentheader) = VALUE bapiache09(
                         username = sy-uname
                       header_txt = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
                        comp_code = <member>-rbukrs
                         doc_date = <member>-bldat
                       pstng_date = _posting_date
*                        fisc_year = <member>-gjahr
                       ref_doc_no = <member>-belnr ).

          DATA(gl_account) = COND #( WHEN <member>-racct = r_racct_ze2[ 1 ]-low THEN r_racct_ze2[ 1 ]-high
                                     WHEN <member>-racct = r_racct_ze2[ 2 ]-low THEN r_racct_ze2[ 2 ]-high
                                     ELSE <member>-racct ) .
          IF <member>-fkart IN r_fkart_exc[].
            IF <member>-rbukrs IN r_bukrs_exc[] .
              IF _0005 = abap_true.
                IF <member>-racct IN r_racct_zi[] AND <member>-bschl = '40'.
                  gl_account = '3102010100'.
                ENDIF.
              ENDIF.
            ENDIF.
          ENDIF.

          posnr += 1.

          DATA(ls_1st_accountgl) = VALUE bapiacgl09(
                      itemno_acc = CONV posnr_acc( |{ posnr }| )
                      gl_account = gl_account
                       item_text = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
                       ref_key_3 = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
                      profit_ctr = COND prctr( WHEN <member>-koart = 'D' THEN <member>-prctr ELSE abap_false )
                        doc_type = lv_doc_type
                       comp_code = <group>-rbukrs
*                       fisc_year = <group>-gjahr
                      pstng_date = _posting_date
                     wbs_element = <member>-posid_edit ).
          INSERT ls_1st_accountgl INTO TABLE lt_accountgl.

          DATA(ls_1st_currencyamount) = VALUE bapiaccr09(
                           itemno_acc = CONV posnr_acc( |{ posnr }| )
                            curr_type = '00'
                             currency = <member>-rhcur
                         currency_iso = <member>-rhcur
                           amt_doccur = - <member>-hsl ).
          INSERT ls_1st_currencyamount INTO TABLE lt_currencyamount REFERENCE INTO DATA(lr_1st_currencyamount).

          APPEND VALUE bapiacextc( field1 = CONV posnr_acc( |{ posnr }| )
                                   field2 = 'ZZ1_RATEIO_PEP_JEI'
                                   field3 = CONV #( 0 ) ) TO lt_extension1.

          APPEND VALUE bapiacextc( field1 = CONV posnr_acc( |{ posnr }| )
                                   field2 = 'XREF1_HD'
                                   field3 = ls_flow-belnr_b1 ) TO lt_extension1.

          DATA(acumulado) = CONV fins_vhcur12( 0 ).
          IF <member>-fkart IN r_fkart_flow[].
            DATA(lt_filter_flow) = FILTER #( mt_flow USING KEY psphi WHERE belnr_k = <member>-belnr AND psphi = <member>-ps_prj_pnr ).
            LOOP AT lt_filter_flow ASSIGNING FIELD-SYMBOL(<filter_flow>)
                                   WHERE racct_a = '2105010050'
                                     AND rateio_a > 0.

              posnr += 1.

              DATA(ls_accountgl) = VALUE bapiacgl09(
                      itemno_acc = CONV posnr_acc( |{ posnr }| )
                      gl_account = gl_account
                       item_text = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
                       ref_key_3 = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
                        doc_type = lv_doc_type
                       comp_code = <group>-rbukrs
*                     fisc_year = <group>-gjahr
                      pstng_date = _posting_date
                     wbs_element = <filter_flow>-posid_edit ).
              INSERT ls_accountgl INTO TABLE lt_accountgl.

              DATA(ls_currencyamount) = VALUE bapiaccr09(
                                   itemno_acc = CONV posnr_acc( |{ posnr }| )
                                    curr_type = '00'
                                     currency = <member>-rhcur
                                 currency_iso = <member>-rhcur
                                   amt_doccur = CONV fins_vhcur12( <member>-hsl * <filter_flow>-rateio_a ) / 100 ).
              AT LAST.
                IF ( lr_1st_currencyamount->amt_doccur -  acumulado ) > 0.
                  ls_currencyamount-amt_doccur = ( acumulado - lr_1st_currencyamount->amt_doccur ).
                ELSE.
                  ls_currencyamount-amt_doccur = - ( lr_1st_currencyamount->amt_doccur - acumulado ).
                ENDIF.
              ENDAT.

              SUBTRACT ls_currencyamount-amt_doccur FROM acumulado.
              INSERT ls_currencyamount INTO TABLE lt_currencyamount.

*** BrunoCappellini-23.04.2024 {
              READ TABLE lt_accountgl
               ASSIGNING FIELD-SYMBOL(<ln_accountgl>)
                WITH KEY itemno_acc = ls_accountgl-itemno_acc
                         gl_account = ls_accountgl-gl_account
                         item_text  = ls_accountgl-item_text
                         BINARY SEARCH.

              IF ( <ln_accountgl> IS ASSIGNED ).

                IF ( <ln_accountgl>-alloc_nmbr IS INITIAL ).
                  CASE <ln_accountgl>-doc_type.
                    WHEN 'ZE'.
                      IF <filter_flow>-rateio_a EQ 0 .
                        <ln_accountgl>-alloc_nmbr = 'R'.
                      ELSEIF <filter_flow>-rateio_a GT 0 .
                        <ln_accountgl>-alloc_nmbr = 'R1'.
                      ENDIF.
                    WHEN 'ZI'.
                      IF <filter_flow>-rateio_a GT 0.
                        <ln_accountgl>-alloc_nmbr = 'R1'.
                      ENDIF.
                    WHEN OTHERS.
                  ENDCASE.
                ENDIF.

              ENDIF.
*** } BrunoCappellini-23.04.2024

              APPEND VALUE bapiacextc( field1 = CONV posnr_acc( |{ posnr }| )
                                       field2 = 'ZZ1_RATEIO_PEP_JEI'
                                       field3 = |{ <filter_flow>-rateio_a DECIMALS = 2 }| ) TO lt_extension1.

              APPEND VALUE bapiacextc( field1 = CONV posnr_acc( |{ posnr }| )
                                       field2 = 'XREF1_HD'
                                       field3 = ls_flow-belnr_b1 ) TO lt_extension1.

            ENDLOOP.

          ELSE.
            DATA(lt_filter) = FILTER #( mt_wbs_elements WHERE psphi = <member>-ps_prj_pnr ).
            LOOP AT lt_filter ASSIGNING FIELD-SYMBOL(<filter>).

              posnr += 1.

              ls_accountgl = VALUE bapiacgl09(
                      itemno_acc = CONV posnr_acc( |{ posnr }| )
                      gl_account = gl_account
                       item_text = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
                       ref_key_3 = |{ <member>-belnr }{ <member>-rbukrs }{ <member>-gjahr }|
                        doc_type = lv_doc_type
                       comp_code = <group>-rbukrs
*                     fisc_year = <group>-gjahr
                      pstng_date = _posting_date
                     wbs_element = <filter>-posid_edit ).
              INSERT ls_accountgl INTO TABLE lt_accountgl.

              ls_currencyamount = VALUE bapiaccr09(
                                   itemno_acc = CONV posnr_acc( |{ posnr }| )
                                    curr_type = '00'
                                     currency = <member>-rhcur
                                 currency_iso = <member>-rhcur
                                   amt_doccur = CONV fins_vhcur12( ( <member>-hsl * <filter>-usr07 ) / 10 ) ).
              AT LAST.
                IF ( lr_1st_currencyamount->amt_doccur -  acumulado ) > 0.
                  ls_currencyamount-amt_doccur = ( acumulado - lr_1st_currencyamount->amt_doccur ).
                ELSE.
                  ls_currencyamount-amt_doccur = - ( lr_1st_currencyamount->amt_doccur - acumulado ).
                ENDIF.
              ENDAT.

              SUBTRACT ls_currencyamount-amt_doccur FROM acumulado.
              INSERT ls_currencyamount INTO TABLE lt_currencyamount.

*** BrunoCappellini-23.04.2024 {
              READ TABLE lt_accountgl
               ASSIGNING <ln_accountgl>
                WITH KEY itemno_acc = ls_accountgl-itemno_acc
                         gl_account = ls_accountgl-gl_account
                         item_text  = ls_accountgl-item_text
                         BINARY SEARCH.

              IF ( <ln_accountgl> IS ASSIGNED ).

                IF ( <ln_accountgl>-alloc_nmbr IS INITIAL ).
                  CASE <ln_accountgl>-doc_type.
                    WHEN 'ZE'.
                      IF <filter>-usr07 EQ 0 .
                        <ln_accountgl>-alloc_nmbr = 'R'.
                      ELSEIF <filter>-usr07 GT 0 .
                        <ln_accountgl>-alloc_nmbr = 'R1'.
                      ENDIF.
                    WHEN 'ZI'.
                      IF <filter>-usr07 GT 0.
                        <ln_accountgl>-alloc_nmbr = 'R1'.
                      ENDIF.
                    WHEN OTHERS.
                  ENDCASE.
                ENDIF.

              ENDIF.
*** } BrunoCappellini-23.04.2024

              APPEND VALUE bapiacextc( field1 = CONV posnr_acc( |{ posnr }| )
                                       field2 = 'ZZ1_RATEIO_PEP_JEI'
                                       field3 = |{ <filter>-usr07 * 10 DECIMALS = 2 }| ) TO lt_extension1.

              APPEND VALUE bapiacextc( field1 = CONV posnr_acc( |{ posnr }| )
                                       field2 = 'XREF1_HD'
                                       field3 = ls_flow-belnr_b1 ) TO lt_extension1.

            ENDLOOP.

          ENDIF.

*          DATA(lt_filter) = FILTER #( mt_wbs_elements WHERE psphi = <member>-ps_prj_pnr ).

        ENDIF.

      ENDLOOP.

      LOG-POINT ID zcr047 FIELDS <group> lt_accountgl lt_currencyamount lt_extension1.


      LOOP AT lt_accountgl ASSIGNING FIELD-SYMBOL(<accountgl_log>).
        DATA(ls_currencyamount_log) = VALUE #( lt_currencyamount[ itemno_acc = <accountgl_log>-itemno_acc ] OPTIONAL ).
        MESSAGE i043 WITH <accountgl_log>-wbs_element
                          <accountgl_log>-gl_account
                          ls_currencyamount_log-amt_doccur
           INTO dummy.
        zcl_ptool_helper=>bal_log_msg_add( iv_handle = _handle ).
        zcl_ptool_helper=>bal_db_save( ).
      ENDLOOP.

      IF error = abap_false.
        IF lt_accountgl[] IS NOT INITIAL AND lt_currencyamount[] IS NOT INITIAL.
          IF _testrun IS INITIAL.
            IF apportionment_post(
              EXPORTING
                is_documentheader = ls_documentheader
              CHANGING
                ct_accountgl      = lt_accountgl
                ct_currencyamount = lt_currencyamount
                ct_extension1     = lt_extension1 ).

              DATA(message) = VALUE #( mt_return[ type = CONV bapi_mtype( `S` )
                                                    id = CONV symsgid( 'RW' )
                                                number = CONV symsgno( '605' ) ] OPTIONAL ).
              <member>-ze_belnr = message-message_v2.
              <member>-message = message-message.
              <member>-icon = icon_release.
              LOG-POINT ID zcr047 FIELDS message.

              GET PARAMETER ID 'BLN' FIELD <member>-ze_belnr.
              GET PARAMETER ID 'BUK' FIELD <member>-ze_bukrs.
              GET PARAMETER ID 'GJR' FIELD <member>-ze_gjahr.

              MODIFY ct_documents
                FROM <member>
                TRANSPORTING ze_belnr ze_gjahr ze_bukrs
                WHERE rldnr = <group>-rldnr
                  AND rbukrs = <group>-rbukrs
                  AND gjahr = <group>-gjahr
                  AND belnr = <group>-belnr.

            ELSE.
              <member>-icon = icon_defect.
              <member>-message = 'Erro do lançamento, acessar a transação SLG1 para detalhes'(001).
            ENDIF.
          ELSE.
            IF apportionment_check(
              EXPORTING
                is_documentheader = ls_documentheader
              CHANGING
                ct_accountgl      = lt_accountgl
                ct_currencyamount = lt_currencyamount
                ct_extension1     = lt_extension1 ).
              message = VALUE #( mt_return[ type = CONV bapi_mtype( `S` )
                                              id = CONV symsgid( 'RW' )
                                          number = CONV symsgno( '605' ) ] OPTIONAL ).
              <member>-ze_belnr = message-message_v2.
              <member>-message = message-message.
              <member>-icon = icon_release.

              GET PARAMETER ID 'BLN' FIELD <member>-ze_belnr.
              GET PARAMETER ID 'BUK' FIELD <member>-ze_bukrs.
              GET PARAMETER ID 'GJR' FIELD <member>-ze_gjahr.
              MODIFY ct_documents
                FROM <member>
                TRANSPORTING ze_belnr ze_gjahr ze_bukrs
                WHERE rldnr = <group>-rldnr
                  AND rbukrs = <group>-rbukrs
                  AND gjahr = <group>-gjahr
                  AND belnr = <group>-belnr.
            ELSE.
              <member>-icon = icon_defect.
              <member>-message = 'Erro do lançamento, acessar a transação SLG1 para detalhes'(001).
            ENDIF.
          ENDIF.
        ENDIF.
      ENDIF.

      CLEAR:
        ls_documentheader,
        lt_accountgl,
        lt_currencyamount,
        lt_extension1.

    ENDLOOP.

  ENDMETHOD.
ENDCLASS.
