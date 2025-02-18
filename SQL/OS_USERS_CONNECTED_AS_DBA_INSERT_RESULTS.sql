BEGIN
	APP_OS_USERS_CONNECTED_AS_DBA.PKG_OS_USERS_CONNECTED_AS_DBA.INSERT_RESULT (nDBID 									=> :dbid,
																																				vUNIFIED_AUDIT_POLICIES => :unified_audit_policies,
																																				vOS_USERNAME						=> :os_username,
																																				vUSERHOST								=> :userhost,
																																				vCLIENT_PROGRAM_NAME		=> :client_program_name,
																																				vCURRENT_USER						=> :current_user,
																																				nRECORDS								=> :records,
																																				dCONNECT_DATE						=> :connect_date);
END;