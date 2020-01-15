#!../../bin/linux-x86_64/echo

< envPaths

cd "${TOP}"

dbLoadDatabase "dbd/echo.dbd"
echo_registerRecordDeviceDriver pdbbase

drvAsynIPPortConfigure("P0","unix:///tmp/socket")

dbLoadRecords("db/echo.db","user=carneirofc,PORT=P0,A=0,P=echo")


cd "${TOP}/iocBoot/${IOC}"
iocInit

