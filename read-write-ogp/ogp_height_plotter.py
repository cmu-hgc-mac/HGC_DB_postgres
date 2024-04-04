# import pandas as pd
# import os, sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.colors as cls
import xlrd

# from hexmap.plot_summary import *

def read_file(loc,sheetname):
    wb=xlrd.open_workbook(loc)
    globals()[f"{sheetname}"]=wb.sheet_by_index(0)

def loadsheet(filenames):
    sheet = np.array([i.split(".xls")[0].split('/')[-1] for i in filenames])
    for i in range(len(filenames)):
        read_file(f'{filenames[i]}',sheet[i])
    return sheet

def displaysheet(sheet,row):
    for i in range(row):
        actual = globals()[f"{sheet}"].cell_value(i,2)
        print(actual)

def Height(sheetname,key = 'Thick'):   ### change the key here for searching where is the first Height in the excel
    row = globals()[f"{sheetname}"].nrows
    #print("total number of row is", row)
    for i in range(row):
        actual = str(globals()[f'{sheetname}'].cell_value(i,2))
        if (key in actual):
            print(f"First Height locates at line {i} in {sheetname}.xls")
            print(f"Row name: {actual}")
            print()
            height1 = i
            break
    #height1 = i
    globals()[f'Height_{sheetname}']=np.zeros((3,25))
    if sheetname == '815 unconstrained flatness':
        lines=[24,25,26]
    else:
        lines=[height1,height1+1,height1+2]
    #lines = [20,21,22]
    for i in range(0,25):
    #print(i)
        x=float(globals()[f'{sheetname}'].cell_value(lines[0]+i*5,5))
        y=float(globals()[f'{sheetname}'].cell_value(lines[1]+i*5,5))
        z=float(globals()[f'{sheetname}'].cell_value(lines[2]+i*5,5))
        globals()[f'Height_{sheetname}'][0,i]=x
        globals()[f'Height_{sheetname}'][1,i]=y
        globals()[f'Height_{sheetname}'][2,i]=z
        #print(x)
    return globals()[f'Height_{sheetname}']

def Flat(sheetname,key = 'Thick'):   ### change the key here for searching where is the first Height in the excel
    row = globals()[f"{sheetname}"].nrows
    #print("total number of row is", row)
    for i in range(row):
        actual = str(globals()[f'{sheetname}'].cell_value(i,2))
        if (key in actual):
            print(f"First Height locates at line {i} in {sheetname}.xls")
            print(f"Row name: {actual}")
            print()
            height1 = i
            break
    #height1 = i
    globals()[f'Height_{sheetname}']=np.zeros((3,25))
    if sheetname == '815 unconstrained flatness':
        lines=[24,25,26]
    else:
        lines=[height1,height1+1,height1+2]
    #lines = [20,21,22]
    # for i in range(25):
    #     print(float(globals()[f'{sheetname}'].cell_value(lines[0]+i*5,5)))
    i = 25
    return float(globals()[f'{sheetname}'].cell_value(4+lines[0]+i*5,5))

def getDate(sheetname,key = 'Date'):   ### change the key here for searching where is the first Height in the excel
    row = globals()[f"{sheetname}"].nrows
    print('row acquired')
    #print("total number of row is", row)
    for i in range(row):
        print(i, str(globals()[f'{sheetname}'].cell_value(i,0)), str(globals()[f'{sheetname}'].cell_value(i,1)), str(globals()[f'{sheetname}'].cell_value(i,2)))
        actual = str(globals()[f'{sheetname}'].cell_value(i,2))
        if (key in actual):
            print(f"First Date locates at line {i} in {sheetname}.xls")
            print(f"Row name: {actual}")
            print()
            height1 = i
            break
    #height1 = i
    globals()[f'Height_{sheetname}']=np.zeros((3,25))
    if sheetname == '815 unconstrained flatness':
        lines=[24,25,26]
    else:
        lines=[height1,height1+1,height1+2]
    #lines = [20,21,22]
    for i in range(0,25):
    #print(i)
        x=float(globals()[f'{sheetname}'].cell_value(lines[0]+i*5,5))
        y=float(globals()[f'{sheetname}'].cell_value(lines[1]+i*5,5))
        z=float(globals()[f'{sheetname}'].cell_value(lines[2]+i*5,5))
        globals()[f'Height_{sheetname}'][0,i]=x
        globals()[f'Height_{sheetname}'][1,i]=y
        globals()[f'Height_{sheetname}'][2,i]=z
        #print(x)
    return globals()[f'Height_{sheetname}']

def AppendTime(sheetnames,key = 'Date'):
    All_Heights=[]
    for j in range(len(sheetnames)):
      All_Heights.append(np.array(getDate(sheetnames[j],key)))
    return All_Heights

def AppendHeights(sheetnames,key = 'Thick', mappings = None):
    All_Heights=[]
    for j in range(len(sheetnames)):
        if mappings[j] is not None:
            All_Heights.append(np.array(Height(sheetnames[j],key))[:,mappings[j]])
        else:
            All_Heights.append(np.array(Height(sheetnames[j],key)))
    return All_Heights

def AppendFlats(sheetnames,key = 'Thick'):
    All_Flats=[]
    for j in range(len(sheetnames)):
        All_Flats.append(np.array(Flat(sheetnames[j],key)))
    return All_Flats

def vec_angle(x,y):
    angle_arctan = np.degrees(np.arctan2(y,x))
    return angle_arctan

def vec_rotate(old_x, old_y, old_angle, new_angle = 120):
    rad = np.radians(new_angle - old_angle)
    new_x = old_x*np.cos(rad)-old_y*np.sin(rad)
    new_y = old_x*np.sin(rad)+old_y*np.cos(rad)
    return new_x, new_y

def plot2d(x, y, zheight, limit = 0, vmini=1.05, vmaxi=4.5, center = 0, rotate = 0 , new_angle = 120, details = 0, title="", savename="", value = 1, day_count = None, mod_flat = None, show_plot = True):

    # print('sorting for consisitency')
    # argx = np.argsort(x)
    # x, y, zheight = x[argx], y[argx], zheight[argx]
    # argy = np.argsort(y)
    # x, y, zheight = x[argy], y[argy], zheight[argy]
    mean_h = np.mean(zheight)
    std_h = np.std(zheight)
    max_h = max(zheight)
    min_h = min(zheight)
    print(f"Average Height is {mean_h:.3f} mm")
    print(f"Maximum Height is {max_h:.3f} mm")
    print(f"Minimum Height is {min_h:.3f} mm")
    print(f"Height --> {mean_h:.3f} + ({max_h - mean_h:.3f}) - ({mean_h - min_h:.3f}) mm")
    print()
    if center != 0:
        if (type(center) is int) and (center >0 and center <26):
            ######## Last point is the center if given 25
            x = x- x[center-1]
            y = y- y[center-1]
            if details == 1:
                print(f"Point {center} center at (0,0)")
        else:
            print("Please give a integer between 1 and 25 for center point") 
    else:
        if details == 1:
            print("No center")

    if rotate != 0:
        if (type(rotate) is int) and (rotate > 0 and rotate <26):
            rotate_angle = vec_angle(x[rotate-1], y[rotate-1])
        else:
            rotate_angle = 0
            # print("Please give a integer between 1 and 25 for rotate point")   
        for i in range(len(x)):
            x[i], y[i] = vec_rotate(x[i],y[i],rotate_angle, new_angle)
        if details == 1:
            print(f"Point {rotate} originally at {rotate_angle:.2f} degrees")
            print(f"Point {rotate} rotates to {new_angle:.2f} degrees")

    else:
        if details == 1:
            print("No rotation")

    fig=plt.figure(dpi=150, figsize=(9,5))
    axs=fig.add_subplot(111); axs.set_aspect('equal')
    
    
    if value == 1:
        image=axs.hexbin(x,y,zheight,gridsize=20, vmin = vmini, vmax = vmaxi, cmap=plt.cm.coolwarm)
        norm = cls.Normalize(vmin=vmini, vmax=vmaxi)
        sm = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.coolwarm)
        cb=plt.colorbar(sm, ax= axs); cb.minorticks_on()
        for i in range(len(zheight)):
            axs.annotate(f"${zheight[i]:.3f}$",(x[i],y[i]),color = "black", fontsize = 9)
            # axs.annotate(f"${i}$",(x[i]-2.5,y[i]-5.5),color = "green", fontsize = 7)
            # if zheight[i]-mean_h < 0:
            #     axs.annotate(f"(${(zheight[i]-mean_h):.3f}$)",(x[i]-2.5,y[i]-5.5),color = "blue", fontsize = 7)
            # else:
            #     axs.annotate(f"(${(zheight[i]-mean_h):.3f}$)",(x[i]-2.5,y[i]-5.5),color = "red", fontsize = 7)
    # elif value == 0:
    #     image=axs.hexbin(x,y,zheight-mean_h,gridsize=20, vmin = vmini, vmax = vmaxi, cmap=plt.cm.coolwarm)
    #     norm = cls.Normalize(vmin=vmini, vmax=vmaxi)
    #     for i in range(len(zheight)):
    #         axs.annotate(f"${(zheight[i]-mean_h):.3f}$",(x[i],y[i]),color = "black")
    else:
        if title == '815 PCB fiducials':
            thickness = np.array([10,11,12,13,14,15,16,17,18,1,2,3,4,5,6,7,8,9,19,20,21,22,23,24,25])
            axs.annotate(f"{thickness[i]}",(x[i],y[i]),color="black")
    #axs.annotate(f"{i+1}",(temp[0][i],temp[1][i]),color="black")
        else:
            axs.annotate(f"{i+1}",(x[i],y[i]),color="black")

    
    #axs.annotate("1", temp[0][0],temp[1][0])
    axs.set_xlabel("x (mm)")
    axs.set_ylabel("y (mm)")
    axs.minorticks_on()
    axs.set_xlim(left=-100, right=100)
    axs.set_ylim(bottom=-100, top=100)
    #axs.set_xticks([-100,-75,-50,-25,0,25,50,75,100])
    #axs.set_yticks([-100,-75,-50,-25,0,25,50,75,100])
    cb.set_label("Height (mm)")
    axs.set_title(title)
    if mod_flat is not None:
        textstr = '\n'.join((f'mean: {mean_h:.3f} mm',f'std:     {std_h:.3f} mm','', f'height: {mean_h:.3f} mm', f'       $+$ ({max_h - mean_h:.3f}) mm', f'       $-$ ({mean_h - min_h:.3f}) mm',
                            '',f'$\Delta$H = {max_h - min_h:.3f} mm','', f'maxH: {max_h:.3f} mm', f'minH:  {min_h:.3f} mm','', f'flatness: {mod_flat:.3f}'))
    else:
        textstr = '\n'.join((f'mean: {mean_h:.3f} mm',f'std:     {std_h:.3f} mm','', f'height: {mean_h:.3f} mm', f'       $+$ ({max_h - mean_h:.3f}) mm', f'       $-$ ({mean_h - min_h:.3f}) mm',
                         '',f'$\Delta$H = {max_h - min_h:.3f} mm','', f'maxH: {max_h:.3f} mm', f'minH:  {min_h:.3f} mm',''))
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
    axs.text(1.3, 1.0, textstr, transform=axs.transAxes, fontsize=10, verticalalignment='top', bbox=props)
    # legendstr = '\n'.join(('*Numbers in brackets represent', 'deviation from mean.'))
    # axs.text(1.3, 0.40, legendstr, transform=axs.transAxes, fontsize=5, verticalalignment='top', color = 'blue')
    if day_count is not None:
        legendstr = '\n'.join((f'Day',f'{day_count}'))
        # legendstr = '\n'.join((f'',f'{day_count}'))
        axs.text(1.3, 0.20, legendstr, transform=axs.transAxes, fontsize=20, verticalalignment='top', color = 'blue')

    if show_plot:
        plt.show(); 
        plt.close()
    
    from io import BytesIO  
    buffer = BytesIO()
    plt.savefig(f"{(savename.split('/'))[-1]}.png", bbox_inches='tight') # uncomment here for saving the 2d plot
    plt.savefig(buffer, format='png', bbox_inches='tight')
    buffer.seek(0)
    plt.close()
    return buffer.read()

def get_data(x, y, zheight, limit = 0, vmini=1.05, vmaxi=4.5, center = 0, rotate = 0 , new_angle = 120, details = 0, title="", savename="", value = 1, day_count = None, mod_flat = None, show_plot = True):

    # print('sorting for consisitency')
    # argx = np.argsort(x)
    # x, y, zheight = x[argx], y[argx], zheight[argx]
    # argy = np.argsort(y)
    # x, y, zheight = x[argy], y[argy], zheight[argy]
    mean_h = np.mean(zheight)
    std_h = np.std(zheight)
    max_h = max(zheight)
    min_h = min(zheight)
    print(f"Average Height is {mean_h:.3f} mm")
    print(f"Maximum Height is {max_h:.3f} mm")
    print(f"Minimum Height is {min_h:.3f} mm")
    print(f"Height --> {mean_h:.3f} + ({max_h - mean_h:.3f}) - ({mean_h - min_h:.3f}) mm")
    print()
    if center != 0:
        if (type(center) is int) and (center >0 and center <26):
            ######## Last point is the center if given 25
            x = x- x[center-1]
            y = y- y[center-1]
            if details == 1:
                print(f"Point {center} center at (0,0)")
        else:
            print("Please give a integer between 1 and 25 for center point") 
    else:
        if details == 1:
            print("No center")

    if rotate != 0:
        if (type(rotate) is int) and (rotate > 0 and rotate <26):
            rotate_angle = vec_angle(x[rotate-1], y[rotate-1])
        else:
            rotate_angle = 0
            # print("Please give a integer between 1 and 25 for rotate point")   
        for i in range(len(x)):
            x[i], y[i] = vec_rotate(x[i],y[i],rotate_angle, new_angle)
        if details == 1:
            print(f"Point {rotate} originally at {rotate_angle:.2f} degrees")
            print(f"Point {rotate} rotates to {new_angle:.2f} degrees")

    else:
        if details == 1:
            print("No rotation")

    fig=plt.figure(dpi=150, figsize=(9,5))
    axs=fig.add_subplot(111); axs.set_aspect('equal')
    
    
    if value == 1:
        image=axs.hexbin(x,y,zheight,gridsize=20, vmin = vmini, vmax = vmaxi, cmap=plt.cm.coolwarm)
        norm = cls.Normalize(vmin=vmini, vmax=vmaxi)
        sm = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.coolwarm)
        cb=plt.colorbar(sm, ax= axs); cb.minorticks_on()
        for i in range(len(zheight)):
            axs.annotate(f"${zheight[i]:.3f}$",(x[i],y[i]),color = "black", fontsize = 9)
            # axs.annotate(f"${i}$",(x[i]-2.5,y[i]-5.5),color = "green", fontsize = 7)
            # if zheight[i]-mean_h < 0:
            #     axs.annotate(f"(${(zheight[i]-mean_h):.3f}$)",(x[i]-2.5,y[i]-5.5),color = "blue", fontsize = 7)
            # else:
            #     axs.annotate(f"(${(zheight[i]-mean_h):.3f}$)",(x[i]-2.5,y[i]-5.5),color = "red", fontsize = 7)
    # elif value == 0:
    #     image=axs.hexbin(x,y,zheight-mean_h,gridsize=20, vmin = vmini, vmax = vmaxi, cmap=plt.cm.coolwarm)
    #     norm = cls.Normalize(vmin=vmini, vmax=vmaxi)
    #     for i in range(len(zheight)):
    #         axs.annotate(f"${(zheight[i]-mean_h):.3f}$",(x[i],y[i]),color = "black")
    else:
        if title == '815 PCB fiducials':
            thickness = np.array([10,11,12,13,14,15,16,17,18,1,2,3,4,5,6,7,8,9,19,20,21,22,23,24,25])
            axs.annotate(f"{thickness[i]}",(x[i],y[i]),color="black")
    #axs.annotate(f"{i+1}",(temp[0][i],temp[1][i]),color="black")
        else:
            axs.annotate(f"{i+1}",(x[i],y[i]),color="black")

    
    #axs.annotate("1", temp[0][0],temp[1][0])
    axs.set_xlabel("x (mm)")
    axs.set_ylabel("y (mm)")
    axs.minorticks_on()
    axs.set_xlim(left=-100, right=100)
    axs.set_ylim(bottom=-100, top=100)
    #axs.set_xticks([-100,-75,-50,-25,0,25,50,75,100])
    #axs.set_yticks([-100,-75,-50,-25,0,25,50,75,100])
    cb.set_label("Height (mm)")
    axs.set_title(title)
    if mod_flat is not None:
        textstr = '\n'.join((f'mean: {mean_h:.3f} mm',f'std:     {std_h:.3f} mm','', f'height: {mean_h:.3f} mm', f'       $+$ ({max_h - mean_h:.3f}) mm', f'       $-$ ({mean_h - min_h:.3f}) mm',
                            '',f'$\Delta$H = {max_h - min_h:.3f} mm','', f'maxH: {max_h:.3f} mm', f'minH:  {min_h:.3f} mm','', f'flatness: {mod_flat:.3f}'))
    else:
        textstr = '\n'.join((f'mean: {mean_h:.3f} mm',f'std:     {std_h:.3f} mm','', f'height: {mean_h:.3f} mm', f'       $+$ ({max_h - mean_h:.3f}) mm', f'       $-$ ({mean_h - min_h:.3f}) mm',
                         '',f'$\Delta$H = {max_h - min_h:.3f} mm','', f'maxH: {max_h:.3f} mm', f'minH:  {min_h:.3f} mm',''))
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
    axs.text(1.3, 1.0, textstr, transform=axs.transAxes, fontsize=10, verticalalignment='top', bbox=props)
    # legendstr = '\n'.join(('*Numbers in brackets represent', 'deviation from mean.'))
    # axs.text(1.3, 0.40, legendstr, transform=axs.transAxes, fontsize=5, verticalalignment='top', color = 'blue')
    if day_count is not None:
        legendstr = '\n'.join((f'Day',f'{day_count}'))
        # legendstr = '\n'.join((f'',f'{day_count}'))
        axs.text(1.3, 0.20, legendstr, transform=axs.transAxes, fontsize=20, verticalalignment='top', color = 'blue')

    if show_plot:
        plt.show(); 
        plt.close()
    
    plt.savefig(f"{(savename.split('/'))[-1]}.png", bbox_inches='tight') # uncomment here for saving the 2d plot
    plt.close()

def uploadPostgres(x, y, zheight, mod_flat, title):
    return None

####################################

import os
def check(file):
    if os.path.exists(file):
        return True
    else:
        print('FILEPATHS MAY BE WRONG!! CANNOT FIND ' + file)
        print()
        return False

def searchTrayPin(sheet,keys, details = 0, Tray = 0, points = None):
    if Tray == 1:
        if "TrayNumber" in globals():
            sheet = Traysheets[globals()["TrayNumber"]-1]
        else:
            print("Could not find any Tray number in the OGPSurveyfile by auto detection")
            print("Continue using the GantryTrayfile")
            print()
    Feature = 0
    row = globals()[f"{sheet}"].nrows
    #print(f"total row is {row}")
    for i in range(row):
        actual = str(globals()[f"{sheet}"].cell_value(i,2))
        for j in range(len(keys)):
            if (keys[j] in actual):
                Feature += 1
    print(f"Found {Feature} Feature rows")
    for i in range(row):
        actual = str(globals()[f"{sheet}"].cell_value(i,2))
        for j in range(len(keys)):
            if (keys[j] in actual):
                if Feature == 2:
                    x = float(globals()[f"{sheet}"].cell_value(i,5))
                    y = float(globals()[f"{sheet}"].cell_value(i+1,5))
                    points[f"{keys[j]}.X"] = x
                    points[f"{keys[j]}.Y"] = y
                if Feature == 4:
                    value = float(globals()[f"{sheet}"].cell_value(i,4))
                    if "X" in actual:
                         points[f"{keys[j]}.X"] = value
                    if "Y" in actual:
                        points[f"{keys[j]}.Y"] = value
                if details == 1:
                    print(f"{keys[j]} locates at line {i} in {sheet}.xls")
                    print(f"Row name: {actual}")
                    #print(f"value: {value}")
    print()

def searchSensorFD(sheet,keys,details = 0,Tray = 0,Traykeys=["Tray"], fd = None, points=None):
    row = globals()[f"{sheet}"].nrows
    for i in range(row):
        actual = str(globals()[f'{sheet}'].cell_value(i,2))
        if Tray == 1:
            for k in range(len(Traykeys)):
                if (Traykeys[k] in actual):
                    for c in range(3,6):
                        if "T" in str(globals()[f"{sheet}"].cell_value(i,c)):
                            globals()["TrayNumber"] = int(actual.split("Tray")[-1])
                            print("Auto detect Tray to be", actual)
                            print()
                            break
                    break
        for j in range(len(keys)):
            if (keys[j] in actual):
                x = float(globals()[f'{sheet}'].cell_value(i,5))
                y = float(globals()[f'{sheet}'].cell_value(i+1,5))
                if details == 1:
                    print(f"Sensor Fiducial locates at line {i} in {sheet}.xls")
                    print(f"Row name: {actual}")
                    print(f"x: {x}, y: {y}")
                fd.append(actual)
                points[actual] = np.array([x,y])
                break
    print(f"Found {len(fd)} Sensor Fiducial Points")
    print()
    return fd, points

def plotFD(FDpoints,FDCenter,Center, Off, points, fd, sheetnames = ['','']):
    CenterX = f"{Center}.X"
    CenterY = f"{Center}.Y"
    OffX = f"{Off}.X"
    OffY = f"{Off}.Y"
    if FDpoints == 2:
        Xp = [points[CenterX],points[OffX], points[fd[0]][0],points[fd[1]][0]]
        Yp = [points[CenterY],points[OffY], points[fd[0]][1],points[fd[1]][1]]
        plt.figure(dpi=250)
        plt.plot(Xp,Yp,'o',ms=2)
        plt.arrow(Xp[1],Yp[1],Xp[0]-Xp[1],Yp[0]-Yp[1],lw=0.5,color='g')
        plt.arrow(Xp[2],Yp[2],Xp[3]-Xp[2],Yp[3]-Yp[2],lw=0.5,color='orange')
        plt.plot(FDCenter[0],FDCenter[1],'ro',label='FDCenter',ms=2)
        names = ['P1CenterPin','P1OffcenterPin','FD1','FD2']
        for i in range(len(Xp)): 
            plt.annotate(names[i],(Xp[i],Yp[i]))
        plt.legend()
        plt.xlim(60,160)
        plt.title(sheetnames[1])
        plt.xlabel("x [mm]")
        plt.ylabel("y [mm]")
    if FDpoints == 4:
        Xp = [points[CenterX],points[OffX], points[fd[0]][0],points[fd[1]][0],points[fd[2]][0],points[fd[3]][0]]
        Yp = [points[CenterY],points[OffY], points[fd[0]][1],points[fd[1]][1],points[fd[2]][1],points[fd[3]][1]]
        plt.figure(dpi=250)
        plt.plot(Xp,Yp,'o',ms=2)
        plt.arrow(Xp[1],Yp[1],Xp[0]-Xp[1],Yp[0]-Yp[1],lw=0.5,color='g')
        #plt.plot([(Xp[3]+Xp[2])/2],[(Yp[3]+Yp[2])/2],lw=0.5)
        plt.arrow(Xp[2],Yp[2],Xp[4]-Xp[2],Yp[4]-Yp[2],lw=0.5,color='orange')
        plt.arrow(Xp[3],Yp[3],Xp[5]-Xp[3],Yp[5]-Yp[3],lw=0.5,color='orange')
        plt.plot(FDCenter[0],FDCenter[1],'ro',label='FDCenter',ms=2)
        names = ['P1CenterPin','P1OffcenterPin','FD1','FD2','FD3','FD4']
        for i in range(len(Xp)): 
            plt.annotate(names[i],(Xp[i],Yp[i]))
        plt.legend()
        plt.xlim(60,160)
        plt.title(sheetnames[1])
        plt.xlabel("x [mm]")
        plt.ylabel("y [mm]")
    print()


def angle(points,FDpoints=4,OffCenterPin = "Left", details = 0, plot = 0, Center = None, Off = None, fd = None):
    CenterX = f"{Center}.X"
    CenterY = f"{Center}.Y"
    OffX = f"{Off}.X"
    OffY = f"{Off}.Y"
    
    if (FDpoints != 2) and (FDpoints != 4):
        print(f"{FDpoints} Fiducial Points. Invalid Number. Please use either 2 or 4 Fiducial Points")
        #print("Detailing the Sensor FD points searching process for debugging")
        return 
    else:
        print(f"Using {FDpoints} Fiducial Points ")
    print()
    if (OffCenterPin != "Left") and (OffCenterPin != "Right"):
        print("Invalid OffCenter Pin Position")
        print("Please indicate whether OffCenter Pin is on the Left or Right to the Center Pin")
        #break
        return
    else:
        if OffCenterPin == "Left":
            Pin = np.array([points[CenterX],points[CenterY]]) - np.array([points[OffX],points[OffY]])
            print("OffCenter Pin is on the Left relative to the Center Pin")
        if OffCenterPin == "Right":
            Pin = np.array([points[OffX],points[OffY]]) - np.array([points[CenterX],points[CenterY]]) 
            print("OffCenter Pin is on the Right relative to the Center Pin")
    #elif (OffCenterPin != "Left") and (OffCenterPin != "Right"):
        #print("Please indicate whether OffCenter Pin is on the Left or Right to the Center Pin")
        #break
        print()
        PinCenter = np.array([points[CenterX],points[CenterY]])
        angle_Pin= np.degrees(np.arctan2(Pin[1],Pin[0]))
    #print(f"Pin X' axis is at angle {angle_Pin:.3f} degrees")
    if FDpoints==2:
        FD1 = points[fd[0]]-points[fd[1]]
        if FD1[1] > 0:
            angle_FD1= np.degrees(np.arctan2(FD1[1],FD1[0]))-90
        else:
            angle_FD1= np.degrees(np.arctan2(FD1[1],FD1[0]))+90
        FDCenter = (points[fd[0]]+points[fd[1]])/2
        if details == 1:
            print(f"FD1-2 X'' axis is at angle {angle_FD1:.5f} degrees")
            print()
            print(f"FDCenter at x:{FDCenter[0]:.3f} mm, y:{FDCenter[1]:.3f} mm")
            print(f"PinCenter at x:{PinCenter[0]:.3f} mm, y:{PinCenter[1]:.3f} mm")
        print()
        """XOffset = FDCenter[0]-PinCenter[0]
        YOffset = FDCenter[1]-PinCenter[1]
        print(f"Assembly Survey X Offset: {XOffset:.3f} mm")
        print(f"Assembly Survey Y Offset: {YOffset:.3f} mm")
        print()
        AngleOffset = angle_FD1 - angle_Pin
        print(f"Assembly Survey Rotational Offset is {AngleOffset:.5f} degrees")"""
    if FDpoints==4:
        FD1 = points[fd[0]]-points[fd[2]]
        if FD1[1] > 0:
            angle_FD1= np.degrees(np.arctan2(FD1[1],FD1[0])) -90
        else:
            angle_FD1= np.degrees(np.arctan2(FD1[1],FD1[0])) +90
        FD2 = points[fd[1]]-points[fd[3]]
        FD = (points[fd[0]]+points[fd[1]])/2 - (points[fd[2]]+points[fd[3]])/2
        FDCenter = (points[fd[0]]+points[fd[1]]+points[fd[2]]+points[fd[3]])/4
        if FD2[1] > 0:
            angle_FD2= np.degrees(np.arctan2(FD2[1],FD2[0]))-90
        else:
            angle_FD2= np.degrees(np.arctan2(FD2[1],FD2[0]))+90
        if details == 1:
            print(f"FD1-3 X'' axis is at angle {angle_FD1:.5f} degrees")
            print(f"angle of FD1-3 X'' relative to Assembly Tray Pin X' is {angle_FD1 - angle_Pin:.5f} degrees")
            print(f"FD2-4 X'' axis is at angle {angle_FD2:.5f} degrees")
            print(f"angle of FD2-4 X'' relative to Assembly Tray Pin X' is {angle_FD2 - angle_Pin:.5f} degrees")
            print()
            print(f"FDCenter at x:{FDCenter[0]:.3f} mm, y:{FDCenter[1]:.3f} mm")
            print(f"PinCenter at x:{PinCenter[0]:.3f} mm, y:{PinCenter[1]:.3f} mm")
            print()
        """XOffset = FDCenter[0]-PinCenter[0]
        YOffset = FDCenter[1]-PinCenter[1]
        print(f"Assembly Survey X Offset: {XOffset:.3f} mm")
        print(f"Assembly Survey Y Offset: {YOffset:.3f} mm")
        print()
        #print('arctan Method')
        print()"""
        u_Pin = Pin/np.linalg.norm(Pin)
        angle_Pin= np.degrees(np.arctan2(Pin[1],Pin[0]))
        #if details == 1:
            #print(f"Pin X' axis is at angle {angle_Pin:.3f} degrees")
            #print()
        u_FD = FD/np.linalg.norm(FD)
        angle_FD= np.degrees(np.arctan2(FD[1],FD[0]))-90
        if details == 1:
            print(f"Pin X' axis is at angle {angle_Pin:.5f} degrees")
            print()
            print(f"FD1-4 X'' axis is at angle {angle_FD:.5f} degrees")
        #print(f"Assembly Survey Rotational Offset is {angle_FD - angle_Pin:.5f} degrees")
        #print()
        #angle_FD1= np.degrees(np.arctan2(FD1[1],FD1[0]))
        #print(f"FD1-2 X'' axis is at angle {angle_FD1} degrees")
    XOffset = FDCenter[0]-PinCenter[0]
    YOffset = FDCenter[1]-PinCenter[1]
    print(f"Assembly Survey X Offset: {XOffset:.3f} mm")
    print(f"Assembly Survey Y Offset: {YOffset:.3f} mm")
    print()
    AngleOffset = angle_FD1 - angle_Pin
    print(f"Assembly Survey Rotational Offset is {AngleOffset:.5f} degrees")
    if plot == 1:
        plotFD(FDpoints,FDCenter,Center, Off, points, fd,)
    CenterOffset = np.sqrt(XOffset**2 + YOffset**2)
    return CenterOffset, AngleOffset, XOffset, YOffset



def get_offsets(filenames, Traysheets):
    sheetnames = loadsheet(filenames)
    autoTray = 0
    points = {}
    TrayKeys = ["Tray","T"]
    sensorKeys = ["Sensor", "Corner","P1","FD3","FD6","FDthree","FDsix"]  ### Key words for searching Sensor FD points in the file
    fd=[]

    searchSensorFD(sheetnames[1],sensorKeys,details = 0, Tray = autoTray, Traykeys = TrayKeys, fd=fd,points = points)
    Center = 'P1Center'
    Off = 'P1OffcenterPin'
    keys = [Center, Off]
    traypin=[]

    searchTrayPin(sheetnames[0],keys,details=0,Tray = autoTray, points = points)
    CenterOff, AngleOff,XOffset, YOffset = angle(points,FDpoints=len(fd), OffCenterPin="Left",details=0,plot=1, Center = Center, Off = Off, fd = fd)    
    
    return XOffset, YOffset, AngleOff




import re
import colorama
from colorama import Fore, Style

colorClassify = {'-1': Fore.MAGENTA + 'NO INFO' + Fore.BLACK, 
                       '0': Fore.GREEN + 'GREEN' + Fore.BLACK,
                       '1': Fore.YELLOW + 'YELLOW' + Fore.BLACK, 
                       '2': Fore.RED + 'RED' + Fore.BLACK}
classify = {'-1': 'NO INFO',
                         '0': 'GREEN',
                         '1': 'YELLOW',
                         '2': 'RED'}
degrees = [0.03, 0.06, 90]
centers = [0.050, 0.100, 10.0]

def quality(Center, Rotation, position = "P1", details =0, note = 0):
    '''
    QC designation for different measurements
    Measurement      |         GREEN          |        YELLOW         |          RED          |
    _________________|________________________|_______________________|_______________________|
    Angle of Plac.   |0 < abs(x - 90.) <= 0.03 |0.03 < abs(x - 90.) <= .06| 0.06 < abs(x - 90.)<90| 
    Placement        |      0 < x <= 0.05     |    0.05 < x <= 0.1    |      0.1 < x <= 10.   | 
    Height           |0 < abs(x - Nom) <= 0.05|0.05 <abs(x - Nom)<=0.1|0.1 < abs(x - Nom)<=10.| 
    Max Hght from Nom|      0 < x <= 0.05     |    0.05 < x <= 0.1    |    0.1 < x <= 10.     | 
    Min Hght ffrom Nom|      0 < x <= 0.05     |    0.05 < x <= 0.1    |    0.1 < x <= 10.     | 
    
    '''
    if details == 1:
        print(f"The Center Offset is {Center:.3f} mm")
        print(f"The Rotational Offset is {Rotation:.5f} degrees ")
        print()
        
    for i, p in enumerate(centers):
        if Center < p:
            print(f"The placement in position {position} is {colorClassify[str(i)]}")
            break
        elif Center > centers[-1]:
            print(f"The placement in position {position} is more than {centers[-1]} mm")
            break
            
    for j, d in enumerate(degrees):
        if abs(Rotation) < d:
            print(f"The angle in position {position} is {colorClassify[str(j)]}")
            break
        elif abs(Rotation) > degrees[-1]:
            print(f"The angle in position {position} is more than {degrees[-1]} degree")
            break
    return colorClassify[str(i)], colorClassify[str(j)]
    # if note == 1:
    #     print()
    #     help(QualityControl)
