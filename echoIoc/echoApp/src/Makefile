TOP=../..

include $(TOP)/configure/CONFIG
#----------------------------------------
#  ADD MACRO DEFINITIONS AFTER THIS LINE
#=============================

#=============================
# Build the IOC application

PROD_IOC = echo

DBD += echo.dbd

echo_DBD += base.dbd
#echo_DBD += asSupport.dbd
echo_DBD += calcSupport.dbd
echo_DBD += asyn.dbd
echo_DBD += stream.dbd
echo_DBD += drvAsynIPPort.dbd

#echo_LIBS += autosave
echo_LIBS += stream
echo_LIBS += asyn
echo_LIBS += calc


# echo_registerRecordDeviceDriver.cpp derives from echo.dbd
echo_SRCS += echo_registerRecordDeviceDriver.cpp

# Build the main IOC entry point on workstation OSs.
echo_SRCS_DEFAULT += echoMain.cpp
echo_SRCS_vxWorks += -nil-

# Add support from base/src/vxWorks if needed
#echo_OBJS_vxWorks += $(EPICS_BASE_BIN)/vxComLibrary

# Finally link to the EPICS Base libraries
echo_LIBS += $(EPICS_BASE_IOC_LIBS)

#===========================

include $(TOP)/configure/RULES
#----------------------------------------
#  ADD RULES AFTER THIS LINE

