schemas = {
  "store_sales": [
      ("ss_sold_date_sk", "identifier"),
      ("ss_sold_time_sk", "identifier"),
      ("ss_item_sk", "identifier"),
      ("ss_customer_sk", "identifier"),
      ("ss_cdemo_sk", "identifier"),
      ("ss_hdemo_sk", "identifier"),
      ("ss_addr_sk", "identifier"),
      ("ss_store_sk", "identifier"),
      ("ss_promo_sk", "identifier"),
      ("ss_ticket_number", "identifier"),
      ("ss_quantity", "integer"),
      ("ss_wholesale_cost", "decimal"),
      ("ss_list_price", "decimal"),
      ("ss_sales_price", "decimal"),
      ("ss_ext_discount_amt", "decimal"),
      ("ss_ext_sales_price", "decimal"),
      ("ss_ext_wholesale_cost", "decimal"),
      ("ss_ext_list_price", "decimal"),
      ("ss_ext_tax", "decimal"),
      ("ss_coupon_amt", "decimal"),
      ("ss_net_paid", "decimal"),
      ("ss_net_paid_inc_tax", "decimal"),
      ("ss_net_profit", "decimal"),
    ],
  "store_returns": [
    ("sr_returned_date_sk", "identifier"),
    ("sr_return_time_sk", "identifier"),
    ("sr_item_sk", "identifier"),
    ("sr_customer_sk", "identifier"),
    ("sr_cdemo_sk", "identifier"),
    ("sr_hdemo_sk", "identifier"),
    ("sr_addr_sk", "identifier"),
    ("sr_store_sk", "identifier"),
    ("sr_reason_sk", "identifier"),
    ("sr_ticket_number", "identifier"),
    ("sr_return_quantity", "integer"),
    ("sr_return_amt", "decimal"),
    ("sr_return_tax", "decimal"),
    ("sr_return_amt_inc_tax", "decimal"),
    ("sr_fee", "decimal"),
    ("sr_return_ship_cost", "decimal"),
    ("sr_refunded_cash", "decimal"),
    ("sr_reversed_charge", "decimal"),
    ("sr_store_credit", "decimal"),
    ("sr_net_loss", "decimal"),
  ],
  "catalogue_sales": [
    ("cs_sold_date_sk", "identifier"),
    ("cs_sold_time_sk", "identifier"),
    ("cs_ship_date_sk", "identifier"),
    ("cs_bill_customer_sk", "identifier"),
    ("cs_bill_cdemo_sk", "identifier"),
    ("cs_bill_hdemo_sk", "identifier"),
    ("cs_bill_addr_sk", "identifier"),
    ("cs_ship_customer_sk", "identifier"),
    ("cs_ship_cdemo_sk", "identifier"),
    ("cs_ship_hdemo_sk", "identifier"),
    ("cs_ship_addr_sk", "integer"),
    ("cs_call_center_sk", "decimal"),
    ("cs_catalog_page_sk", "decimal"),
    ("cs_ship_mode_sk", "decimal"),
    ("cs_warehouse_sk", "decimal"),
    ("cs_item_sk", "decimal"),
    ("cs_promo_sk", "decimal"),
    ("cs_order_number", "decimal"),
    ("cs_quantity", "integer"),
    ("cs_wholesale_cost", "decimal"),
    ("cs_list_price", "decimal"),
    ("cs_sales_price", "decimal"),
    ("cs_ext_discount_amt", "decimal"),
    ("cs_ext_sales_price", "decimal"),
    ("cs_ext_wholesale_cost", "decimal"),
    ("cs_ext_list_price", "decimal"),
    ("cs_ext_tax", "decimal"),
    ("cs_coupon_amt", "decimal"),
    ("cs_ext_ship_cost", "decimal"),
    ("cs_net_paid", "decimal"),
    ("cs_net_paid_inc_tax", "decimal"),
    ("cs_net_paid_inc_ship", "decimal"),
    ("cs_net_paid_inc_ship_tax", "decimal"),
    ("cs_net_profit", "decimal")
  ],
  "catalogue_returns": [
    ("cr_returned_date_sk", "identifier"),
    ("cr_returned_time_sk", "identifier"),
    ("cr_item_sk", "identifier"),
    ("cr_refunded_customer_sk", "identifier"),
    ("cr_refunded_cdemo_sk", "identifier"),
    ("cr_refunded_hdemo_sk", "identifier"),
    ("cr_refunded_addr_sk", "identifier"),
    ("cr_returning_customer_sk", "identifier"),
    ("cr_returning_cdemo_sk", "identifier"),
    ("cr_returning_hdemo_sk", "identifier"),
    ("cr_returning_addr_sk", "identifier"),
    ("cr_call_center_sk", "identifier"),
    ("cr_catalog_page_sk", "identifier"),
    ("cr_ship_mode_sk", "identifier"),
    ("cr_warehouse_sk", "identifier"),
    ("cr_reason_sk", "identifier"),
    ("cr_order_number", "identifier"),
    ("cr_return_quantity", "integer"),
    ("cr_return_amount", "integer"),
    ("cr_return_tax", "decimal"),
    ("cr_return_amt_inc_tax", "decimal"),
    ("cr_fee", "decimal"),
    ("cr_return_ship_cost", "decimal"),
    ("cr_refunded_cash", "decimal"),
    ("cr_reversed_charge", "decimal"),
    ("cr_store_credit", "decimal"),
    ("cr_net_loss", "decimal")
  ],
  "web_sales": [
    ("ws_sold_date_sk", "identifier"),
    ("ws_sold_time_sk", "identifier"),
    ("ws_ship_date_sk", "identifier"),
    ("ws_item_sk", "identifier"),
    ("ws_bill_customer_sk", "identifier"),
    ("ws_bill_cdemo_sk", "identifier"),
    ("ws_bill_hdemo_sk", "identifier"),
    ("ws_bill_addr_sk", "identifier"),
    ("ws_ship_customer_sk", "identifier"),
    ("ws_ship_cdemo_sk", "identifier"),
    ("ws_ship_hdemo_sk", "integer"),
    ("ws_ship_addr_sk", "decimal"),
    ("ws_web_page_sk", "decimal"),
    ("ws_web_site_sk", "decimal"),
    ("ws_ship_mode_sk", "decimal"),
    ("ws_warehouse_sk", "decimal"),
    ("ws_promo_sk", "decimal"),
    ("ws_order_number", "decimal"),
    ("ws_quantity", "integer"),
    ("ws_wholesale_cost", "decimal"),
    ("ws_list_price", "decimal"),
    ("ws_sales_price", "decimal"),
    ("ws_ext_discount_amt", "decimal"),
    ("ws_ext_sales_price", "decimal"),
    ("ws_ext_wholesale_cost", "decimal"),
    ("ws_ext_list_price", "decimal"),
    ("ws_ext_tax", "decimal"),
    ("ws_coupon_amt", "decimal"),
    ("ws_ext_ship_cost", "decimal"),
    ("ws_net_paid", "decimal"),
    ("ws_net_paid_inc_tax", "decimal"),
    ("ws_net_paid_inc_ship", "decimal"),
    ("ws_net_paid_inc_ship_tax", "decimal"),
    ("ws_net_profit", "decimal")
  ],
  "web_returns": [
    ("wr_returned_date_sk", "identifier"),
    ("wr_returned_time_sk", "identifier"),
    ("wr_item_sk", "identifier"),
    ("wr_refunded_customer_sk", "identifier"),
    ("wr_refunded_cdemo_sk", "identifier"),
    ("wr_refunded_hdemo_sk", "identifier"),
    ("wr_refunded_addr_sk", "identifier"),
    ("wr_returning_customer_sk", "identifier"),
    ("wr_returning_cdemo_sk", "identifier"),
    ("wr_returning_hdemo_sk", "identifier"),
    ("wr_returning_addr_sk", "identifier"),
    ("wr_web_page_sk", "identifier"),
    ("wr_reason_sk", "identifier"),
    ("wr_order_number", "identifier"),
    ("wr_return_quantity", "integer"),
    ("wr_return_amt", "decimal"),
    ("wr_return_tax", "decimal"),
    ("wr_return_amt_inc_tax", "decimal"),
    ("wr_fee", "decimal"),
    ("wr_return_ship_cost", "decimal"),
    ("wr_refunded_cash", "decimal"),
    ("wr_reversed_charge", "decimal"),
    ("wr_account_credit", "decimal"),
    ("wr_net_loss", "decimal")
  ],
  "inventory": [
    ("inv_date_sk", "identifier"),
    ("inv_item_sk", "identifier"),
    ("inv_warehouse_sk", "identifier"),
    ("inv_quantity_on_hand", "integer")
  ],
  "store": [
    ("s_store_sk", "identifier"),
    ("s_store_id", "char"),
    ("s_rec_start_date", "date"),
    ("s_rec_end_date", "date"),
    ("s_closed_date_sk", "identifier"),
    ("s_store_name", "varchar"),
    ("s_number_employees", "integer"),
    ("s_floor_space", "integer"),
    ("s_hours", "char"),
    ("S_manager", "varchar"),
    ("S_market_id", "integer"),
    ("S_geography_class", "varchar"),
    ("S_market_desc", "varchar"),
    ("s_market_manager", "varchar"),
    ("s_division_id", "integer"),
    ("s_division_name", "varchar"),
    ("s_company_id", "integer"),
    ("s_company_name", "varchar"),
    ("s_street_number", "varchar"),
    ("s_street_name", "varchar"),
    ("s_street_type", "char"),
    ("s_suite_number", "char"),
    ("s_city", "varchar"),
    ("s_county", "varchar"),
    ("s_state", "char"),
    ("s_zip", "char"),
    ("s_country", "varchar"),
    ("s_gmt_offset", "decimal"),
    ("s_tax_percentage", "decimal")
  ],
  "call_center": [
    ("cc_call_center_sk", "identifier"),
    ("cc_call_center_id", "char"),
    ("cc_rec_start_date", "date"),
    ("cc_rec_end_date", "date"),
    ("cc_closed_date_sk", "identifier"),
    ("cc_open_date_sk", "identifier"),
    ("cc_name", "varchar"),
    ("cc_class", "varchar"),
    ("cc_employees", "integer"),
    ("cc_sq_ft", "integer"),
    ("cc_hours", "char"),
    ("cc_manager", "varchar"),
    ("cc_mkt_id", "integer"),
    ("cc_mkt_class", "char"),
    ("cc_mkt_desc", "varchar"),
    ("cc_market_manager", "varchar"),
    ("cc_division", "integer"),
    ("cc_division_name", "varchar"),
    ("cc_company", "integer"),
    ("cc_company_name", "char"),
    ("cc_street_number", "char"),
    ("cc_street_name", "varchar"),
    ("cc_street_type", "char"),
    ("cc_suite_number", "char"),
    ("cc_city", "varchar"),
    ("cc_county", "varchar"),
    ("cc_state", "char"),
    ("cc_zip", "char"),
    ("cc_country", "varchar"),
    ("cc_gmt_offset", "decimal"),
    ("cc_tax_percentage", "decimal"),
  ],
  "catalog_page": [
    ("cp_catalog_page_sk", "identifier"),
    ("cp_catalog_page_id", "char"),
    ("cp_start_date_sk", "identifier"),
    ("cp_end_date_sk", "identifier"),
    ("cp_department", "varchar"),
    ("cp_catalog_number", "integer"),
    ("cp_catalog_page_number", "integer"),
    ("cp_description", "varchar"),
    ("cp_type", "varchar")
  ],
  "web_site": [
    ("web_site_sk", "identifier"),
    ("web_site_id", "char"),
    ("web_rec_start_date", "date"),
    ("web_rec_end_date", "date"),
    ("web_name", "varchar"),
    ("web_open_date_sk", "identifier"),
    ("web_close_date_sk", "identifier"),
    ("web_class", "varchar"),
    ("web_manager", "varchar"),
    ("web_mkt_id", "integer"),
    ("web_mkt_class", "varchar"),
    ("web_mkt_desc", "varchar"),
    ("web_market_manager", "varchar"),
    ("web_company_id", "integer"),
    ("web_company_name", "char"),
    ("web_street_number", "char"),
    ("web_street_name", "varchar"),
    ("web_street_type", "char"),
    ("web_suite_number", "char"),
    ("web_city", "varchar"),
    ("web_county", "varchar"),
    ("web_state", "char"),
    ("web_zip", "char"),
    ("web_country", "varchar"),
    ("web_gmt_offset", "decimal"),
    ("web_tax_percentage", "decimal")
  ],
  "web_page": [
    ("wp_web_page_sk", "identifier"),
    ("wp_web_page_id", "char"),
    ("wp_rec_start_date", "date"),
    ("wp_rec_end_date", "date"),
    ("wp_creation_date_sk", "identifier"),
    ("wp_access_date_sk", "identifier"),
    ("wp_autogen_flag", "char"),
    ("wp_customer_sk", "identifier"),
    ("wp_url", "varchar"),
    ("wp_type", "char"),
    ("wp_char_count", "integer"),
    ("wp_link_count", "integer"),
    ("wp_image_count", "integer"),
    ("wp_max_ad_count", "integer")
  ],
  "warehouse": [
    ("w_warehouse_sk", "identifier"),
    ("w_warehouse_id", "char"),
    ("w_warehouse_name", "varchar"),
    ("w_warehouse_sq_ft", "integer"),
    ("w_street_number", "char"),
    ("w_street_name", "varchar"),
    ("w_street_type", "char"),
    ("w_suite_number", "char"),
    ("w_city", "varchar"),
    ("w_county", "varchar"),
    ("w_state", "char"),
    ("w_zip", "char"),
    ("w_country", "varchar"),
    ("w_gmt_offset", "decimal")
  ],
  "customer": [
    ("c_customer_sk", "identifier"),
    ("c_customer_id", "char"),
    ("c_current_cdemo_sk", "identifier"),
    ("c_current_hdemo_sk", "identifier"),
    ("c_current_addr_sk", "identifier"),
    ("c_first_shipto_date_sk", "identifier"),
    ("c_first_sales_date_sk", "identifier"),
    ("c_salutation", "char"),
    ("c_first_name", "char"),
    ("c_last_name", "char"),
    ("c_preferred_cust_flag", "char"),
    ("c_birth_day", "integer"),
    ("c_birth_month", "integer"),
    ("c_birth_year", "integer"),
    ("c_birth_country", "varchar"),
    ("c_login", "char"),
    ("c_email_address", "char"),
    ("c_last_review_date_sk", "identifier")
  ],
  "customer_address": [
    ("ca_address_sk", "identifier"),
    ("ca_address_id", "char"),
    ("ca_street_number", "char"),
    ("ca_street_name", "varchar"),
    ("ca_street_type", "char"),
    ("ca_suite_number", "char"),
    ("ca_city", "varchar"),
    ("ca_county", "varchar"),
    ("ca_state", "char"),
    ("ca_zip", "char"),
    ("ca_country", "varchar"),
    ("ca_gmt_offset", "decimal"),
    ("ca_location_type", "char")
  ],
  "customer_demographics": [
    ("cd_demo_sk", "identifier"),
    ("cd_gender", "char"),
    ("cd_marital_status", "char"),
    ("cd_education_status", "char"),
    ("cd_purchase_estimate", "integer"),
    ("cd_credit_rating", "char"),
    ("cd_dep_count", "integer"),
    ("cd_dep_employed_count", "integer"),
    ("cd_dep_college_count", "integer")
  ],
  "date_dim": [
    ("d_date_sk", "identifier"),
    ("d_date_id", "char"),
    ("d_date", "date"),
    ("d_month_seq", "integer"),
    ("d_week_seq", "integer"),
    ("d_quarter_seq", "integer"),
    ("d_year", "integer"),
    ("d_dow", "integer"),
    ("d_moy", "integer"),
    ("d_dom", "integer"),
    ("d_qoy", "integer"),
    ("d_fy_year", "integer"),
    ("d_fy_quarter_seq", "integer"),
    ("d_fy_week_seq", "integer"),
    ("d_day_name", "char"),
    ("d_quarter_name", "char"),
    ("d_holiday", "char"),
    ("d_weekend", "char"),
    ("d_following_holiday", "char"),
    ("d_first_dom", "integer"),
    ("d_last_dom", "integer"),
    ("d_same_day_ly", "integer"),
    ("d_same_day_lq", "integer"),
    ("d_current_day", "char"),
    ("d_current_week", "char"),
    ("d_current_month", "char"),
    ("d_current_quarter", "char"),
    ("d_current_year", "char")
  ],
  "household_demographics": [
    ("hd_demo_sk", "identifier"),
    ("hd_income_band_sk", "identifier"),
    ("hd_buy_potential", "char"),
    ("hd_dep_count", "integer"),
    ("hd_vehicle_count", "integer")
  ],
  "item": [
    ("i_item_sk", "identifier"),
    ("i_item_id", "char"),
    ("i_rec_start_date", "date"),
    ("i_rec_end_date", "date"),
    ("i_item_desc", "varchar"),
    ("i_current_price", "decimal"),
    ("i_wholesale_cost", "decimal"),
    ("i_brand_id", "integer"),
    ("i_brand", "char"),
    ("i_class_id", "integer"),
    ("i_class", "char"),
    ("i_category_id", "integer"),
    ("i_category", "char"),
    ("i_manufact_id", "integer"),
    ("i_manufact", "char"),
    ("i_size", "char"),
    ("i_formulation", "char"),
    ("i_color", "char"),
    ("i_units", "char"),
    ("i_container", "char"),
    ("i_manager_id", "integer"),
    ("i_product_name", "char")
  ],
  "income_band": [
    ("ib_income_band_sk", "identifier"),
    ("ib_lower_bound", "integer"),
    ("ib_upper_bound", "integer")
  ],
  "promotion": [
    ("p_promo_sk", "identifier"),
    ("p_promo_id", "char"),
    ("p_start_date_sk", "identifier"),
    ("p_end_date_sk", "identifier"),
    ("p_item_sk", "identifier"),
    ("p_cost", "decimal"),
    ("p_response_target", "integer"),
    ("p_promo_name", "char"),
    ("p_channel_dmail", "char"),
    ("p_channel_email", "char"),
    ("p_channel_catalog", "char"),
    ("p_channel_tv", "char"),
    ("p_channel_radio", "char"),
    ("p_channel_press", "char"),
    ("p_channel_event", "char"),
    ("p_channel_demo", "char"),
    ("p_channel_details", "varchar"),
    ("p_purpose", "char"),
    ("p_discount_active", "char")
  ],
  "reason": [
    ("r_reason_sk", "identifier"),
    ("r_reason_id", "char"),
    ("r_reason_desc", "char")
  ],
  "ship_mode": [
    ("sm_ship_mode_sk", "identifier"),
    ("sm_ship_mode_id", "char"),
    ("sm_type", "char"),
    ("sm_code", "char"),
    ("sm_carrier", "char"),
    ("sm_contract", "char")
  ],
  "time_dim": [
    ("t_time_sk", "identifier"),
    ("t_time_id", "char"),
    ("t_time", "integer"),
    ("t_hour", "integer"),
    ("t_minute", "integer"),
    ("t_second", "integer"),
    ("t_am_pm", "char"),
    ("t_shift", "char"),
    ("t_sub_shift", "char"),
    ("t_meal_time", "char")
  ],
  "dsdgen_version": [
    ("dv_version", "varchar"),
    ("dv_create_date", "date"),
    ("dv_create_time", "time"),
    ("dv_cmdline_args", "varchar")
  ]
}
