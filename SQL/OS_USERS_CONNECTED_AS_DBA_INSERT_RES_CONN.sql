BEGIN
	APP_OS_USERS_CONNECTED_AS_DBA.PKG_OS_USERS_CONNECTED_AS_DBA.STOP_DB(nDBID 			=> :dbid,
																																		 vERRORS	 		=> :error_text, 
																																		 vSTATUS	 		=> :status);
END;