from config import *
import random 

##  alias random.randint to make reference to the TPC-C spec obvious
TPCC_random = random.randint

def TPCC_NU_random(x, min, max):
    '''return a non-uniform random number using the TPC-C algorithm'''

    value = ((TPCC_random(0,x) | TPCC_random(min,max)) + 
        TPCC_random(0,x)) % (max-min+1) + min
    return value


def get_itemID():
    ID = TPCC_NU_random(8191,1,CONFIGITEMCOUNT)
    return ID


def get_customerID():
    ID = TPCC_NU_random(1023,1,CONFIGCUSTPERDIST)
    return ID


def get_lastname():
    '''return a randomly select last name'''

    ##  the following is a list of all the distinct names that appear
    ##  in the customer table
    allnames = [
    'ABLEABLEABLE', 'ABLEABLEANTI', 'ABLEABLEATION', 'ABLEABLEBAR',
    'ABLEABLECALLY', 'ABLEABLEEING', 'ABLEABLEESE', 'ABLEABLEOUGHT',
    'ABLEABLEPRES', 'ABLEABLEPRI', 'ABLEANTIABLE', 'ABLEANTIANTI',
    'ABLEANTIATION', 'ABLEANTIBAR', 'ABLEANTICALLY', 'ABLEANTIEING',
    'ABLEANTIESE', 'ABLEANTIOUGHT', 'ABLEANTIPRES', 'ABLEANTIPRI',
    'ABLEATIONABLE', 'ABLEATIONANTI', 'ABLEATIONATION', 'ABLEATIONBAR',
    'ABLEATIONCALLY', 'ABLEATIONEING', 'ABLEATIONESE', 'ABLEATIONOUGHT',
    'ABLEATIONPRES', 'ABLEATIONPRI', 'ABLEBARABLE', 'ABLEBARANTI',
    'ABLEBARATION', 'ABLEBARBAR', 'ABLEBARCALLY', 'ABLEBAREING', 'ABLEBARESE',
    'ABLEBAROUGHT', 'ABLEBARPRES', 'ABLEBARPRI', 'ABLECALLYABLE',
    'ABLECALLYANTI', 'ABLECALLYATION', 'ABLECALLYBAR', 'ABLECALLYCALLY',
    'ABLECALLYEING', 'ABLECALLYESE', 'ABLECALLYOUGHT', 'ABLECALLYPRES',
    'ABLECALLYPRI', 'ABLEEINGABLE', 'ABLEEINGANTI', 'ABLEEINGATION',
    'ABLEEINGBAR', 'ABLEEINGCALLY', 'ABLEEINGEING', 'ABLEEINGESE',
    'ABLEEINGOUGHT', 'ABLEEINGPRES', 'ABLEEINGPRI', 'ABLEESEABLE',
    'ABLEESEANTI', 'ABLEESEATION', 'ABLEESEBAR', 'ABLEESECALLY',
    'ABLEESEEING', 'ABLEESEESE', 'ABLEESEOUGHT', 'ABLEESEPRES', 'ABLEESEPRI',
    'ABLEOUGHTABLE', 'ABLEOUGHTANTI', 'ABLEOUGHTATION', 'ABLEOUGHTBAR',
    'ABLEOUGHTCALLY', 'ABLEOUGHTEING', 'ABLEOUGHTESE', 'ABLEOUGHTOUGHT',
    'ABLEOUGHTPRES', 'ABLEOUGHTPRI', 'ABLEPRESABLE', 'ABLEPRESANTI',
    'ABLEPRESATION', 'ABLEPRESBAR', 'ABLEPRESCALLY', 'ABLEPRESEING',
    'ABLEPRESESE', 'ABLEPRESOUGHT', 'ABLEPRESPRES', 'ABLEPRESPRI',
    'ABLEPRIABLE', 'ABLEPRIANTI', 'ABLEPRIATION', 'ABLEPRIBAR',
    'ABLEPRICALLY', 'ABLEPRIEING', 'ABLEPRIESE', 'ABLEPRIOUGHT',
    'ABLEPRIPRES', 'ABLEPRIPRI', 'ANTIABLEABLE', 'ANTIABLEANTI',
    'ANTIABLEATION', 'ANTIABLEBAR', 'ANTIABLECALLY', 'ANTIABLEEING',
    'ANTIABLEESE', 'ANTIABLEOUGHT', 'ANTIABLEPRES', 'ANTIABLEPRI',
    'ANTIANTIABLE', 'ANTIANTIANTI', 'ANTIANTIATION', 'ANTIANTIBAR',
    'ANTIANTICALLY', 'ANTIANTIEING', 'ANTIANTIESE', 'ANTIANTIOUGHT',
    'ANTIANTIPRES', 'ANTIANTIPRI', 'ANTIATIONABLE', 'ANTIATIONANTI',
    'ANTIATIONATION', 'ANTIATIONBAR', 'ANTIATIONCALLY', 'ANTIATIONEING',
    'ANTIATIONESE', 'ANTIATIONOUGHT', 'ANTIATIONPRES', 'ANTIATIONPRI',
    'ANTIBARABLE', 'ANTIBARANTI', 'ANTIBARATION', 'ANTIBARBAR',
    'ANTIBARCALLY', 'ANTIBAREING', 'ANTIBARESE', 'ANTIBAROUGHT',
    'ANTIBARPRES', 'ANTIBARPRI', 'ANTICALLYABLE', 'ANTICALLYANTI',
    'ANTICALLYATION', 'ANTICALLYBAR', 'ANTICALLYCALLY', 'ANTICALLYEING',
    'ANTICALLYESE', 'ANTICALLYOUGHT', 'ANTICALLYPRES', 'ANTICALLYPRI',
    'ANTIEINGABLE', 'ANTIEINGANTI', 'ANTIEINGATION', 'ANTIEINGBAR',
    'ANTIEINGCALLY', 'ANTIEINGEING', 'ANTIEINGESE', 'ANTIEINGOUGHT',
    'ANTIEINGPRES', 'ANTIEINGPRI', 'ANTIESEABLE', 'ANTIESEANTI',
    'ANTIESEATION', 'ANTIESEBAR', 'ANTIESECALLY', 'ANTIESEEING', 'ANTIESEESE',
    'ANTIESEOUGHT', 'ANTIESEPRES', 'ANTIESEPRI', 'ANTIOUGHTABLE',
    'ANTIOUGHTANTI', 'ANTIOUGHTATION', 'ANTIOUGHTBAR', 'ANTIOUGHTCALLY',
    'ANTIOUGHTEING', 'ANTIOUGHTESE', 'ANTIOUGHTOUGHT', 'ANTIOUGHTPRES',
    'ANTIOUGHTPRI', 'ANTIPRESABLE', 'ANTIPRESANTI', 'ANTIPRESATION',
    'ANTIPRESBAR', 'ANTIPRESCALLY', 'ANTIPRESEING', 'ANTIPRESESE',
    'ANTIPRESOUGHT', 'ANTIPRESPRES', 'ANTIPRESPRI', 'ANTIPRIABLE',
    'ANTIPRIANTI', 'ANTIPRIATION', 'ANTIPRIBAR', 'ANTIPRICALLY',
    'ANTIPRIEING', 'ANTIPRIESE', 'ANTIPRIOUGHT', 'ANTIPRIPRES',
    'ANTIPRIPRI', 'ATIONABLEABLE', 'ATIONABLEANTI', 'ATIONABLEATION',
    'ATIONABLEBAR', 'ATIONABLECALLY', 'ATIONABLEEING', 'ATIONABLEESE',
    'ATIONABLEOUGHT', 'ATIONABLEPRES', 'ATIONABLEPRI', 'ATIONANTIABLE',
    'ATIONANTIANTI', 'ATIONANTIATION', 'ATIONANTIBAR', 'ATIONANTICALLY',
    'ATIONANTIEING', 'ATIONANTIESE', 'ATIONANTIOUGHT', 'ATIONANTIPRES',
    'ATIONANTIPRI', 'ATIONATIONABLE', 'ATIONATIONANTI', 'ATIONATIONATION',
    'ATIONATIONBAR', 'ATIONATIONCALLY', 'ATIONATIONEING', 'ATIONATIONESE',
    'ATIONATIONOUGHT', 'ATIONATIONPRES', 'ATIONATIONPRI', 'ATIONBARABLE',
    'ATIONBARANTI', 'ATIONBARATION', 'ATIONBARBAR', 'ATIONBARCALLY',
    'ATIONBAREING', 'ATIONBARESE', 'ATIONBAROUGHT', 'ATIONBARPRES',
    'ATIONBARPRI', 'ATIONCALLYABLE', 'ATIONCALLYANTI', 'ATIONCALLYATION',
    'ATIONCALLYBAR', 'ATIONCALLYCALLY', 'ATIONCALLYEING', 'ATIONCALLYESE',
    'ATIONCALLYOUGHT', 'ATIONCALLYPRES', 'ATIONCALLYPRI', 'ATIONEINGABLE',
    'ATIONEINGANTI', 'ATIONEINGATION', 'ATIONEINGBAR', 'ATIONEINGCALLY',
    'ATIONEINGEING', 'ATIONEINGESE', 'ATIONEINGOUGHT', 'ATIONEINGPRES',
    'ATIONEINGPRI', 'ATIONESEABLE', 'ATIONESEANTI', 'ATIONESEATION',
    'ATIONESEBAR', 'ATIONESECALLY', 'ATIONESEEING', 'ATIONESEESE',
    'ATIONESEOUGHT', 'ATIONESEPRES', 'ATIONESEPRI', 'ATIONOUGHTABLE',
    'ATIONOUGHTANTI', 'ATIONOUGHTATION', 'ATIONOUGHTBAR', 'ATIONOUGHTCALLY',
    'ATIONOUGHTEING', 'ATIONOUGHTESE', 'ATIONOUGHTOUGHT', 'ATIONOUGHTPRES',
    'ATIONOUGHTPRI', 'ATIONPRESABLE', 'ATIONPRESANTI', 'ATIONPRESATION',
    'ATIONPRESBAR', 'ATIONPRESCALLY', 'ATIONPRESEING', 'ATIONPRESESE',
    'ATIONPRESOUGHT', 'ATIONPRESPRES', 'ATIONPRESPRI', 'ATIONPRIABLE',
    'ATIONPRIANTI', 'ATIONPRIATION', 'ATIONPRIBAR', 'ATIONPRICALLY',
    'ATIONPRIEING', 'ATIONPRIESE', 'ATIONPRIOUGHT', 'ATIONPRIPRES',
    'ATIONPRIPRI', 'BARABLEABLE', 'BARABLEANTI', 'BARABLEATION',
    'BARABLEBAR', 'BARABLECALLY', 'BARABLEEING', 'BARABLEESE', 'BARABLEOUGHT',
    'BARABLEPRES', 'BARABLEPRI', 'BARANTIABLE', 'BARANTIANTI', 'BARANTIATION',
    'BARANTIBAR', 'BARANTICALLY', 'BARANTIEING', 'BARANTIESE', 'BARANTIOUGHT',
    'BARANTIPRES', 'BARANTIPRI', 'BARATIONABLE', 'BARATIONANTI',
    'BARATIONATION', 'BARATIONBAR', 'BARATIONCALLY', 'BARATIONEING',
    'BARATIONESE', 'BARATIONOUGHT', 'BARATIONPRES', 'BARATIONPRI',
    'BARBARABLE', 'BARBARANTI', 'BARBARATION', 'BARBARBAR', 'BARBARCALLY',
    'BARBAREING', 'BARBARESE', 'BARBAROUGHT', 'BARBARPRES', 'BARBARPRI',
    'BARCALLYABLE', 'BARCALLYANTI', 'BARCALLYATION', 'BARCALLYBAR',
    'BARCALLYCALLY', 'BARCALLYEING', 'BARCALLYESE', 'BARCALLYOUGHT',
    'BARCALLYPRES', 'BARCALLYPRI', 'BAREINGABLE', 'BAREINGANTI',
    'BAREINGATION', 'BAREINGBAR', 'BAREINGCALLY', 'BAREINGEING',
    'BAREINGESE', 'BAREINGOUGHT', 'BAREINGPRES', 'BAREINGPRI', 'BARESEABLE',
    'BARESEANTI', 'BARESEATION', 'BARESEBAR', 'BARESECALLY', 'BARESEEING',
    'BARESEESE', 'BARESEOUGHT', 'BARESEPRES', 'BARESEPRI', 'BAROUGHTABLE',
    'BAROUGHTANTI', 'BAROUGHTATION', 'BAROUGHTBAR', 'BAROUGHTCALLY',
    'BAROUGHTEING', 'BAROUGHTESE', 'BAROUGHTOUGHT', 'BAROUGHTPRES',
    'BAROUGHTPRI', 'BARPRESABLE', 'BARPRESANTI', 'BARPRESATION',
    'BARPRESBAR', 'BARPRESCALLY', 'BARPRESEING', 'BARPRESESE',
    'BARPRESOUGHT', 'BARPRESPRES', 'BARPRESPRI', 'BARPRIABLE',
    'BARPRIANTI', 'BARPRIATION', 'BARPRIBAR', 'BARPRICALLY', 'BARPRIEING',
    'BARPRIESE', 'BARPRIOUGHT', 'BARPRIPRES', 'BARPRIPRI', 'CALLYABLEABLE',
    'CALLYABLEANTI', 'CALLYABLEATION', 'CALLYABLEBAR', 'CALLYABLECALLY',
    'CALLYABLEEING', 'CALLYABLEESE', 'CALLYABLEOUGHT', 'CALLYABLEPRES',
    'CALLYABLEPRI', 'CALLYANTIABLE', 'CALLYANTIANTI', 'CALLYANTIATION',
    'CALLYANTIBAR', 'CALLYANTICALLY', 'CALLYANTIEING', 'CALLYANTIESE',
    'CALLYANTIOUGHT', 'CALLYANTIPRES', 'CALLYANTIPRI', 'CALLYATIONABLE',
    'CALLYATIONANTI', 'CALLYATIONATION', 'CALLYATIONBAR', 'CALLYATIONCALLY',
    'CALLYATIONEING', 'CALLYATIONESE', 'CALLYATIONOUGHT', 'CALLYATIONPRES',
    'CALLYATIONPRI', 'CALLYBARABLE', 'CALLYBARANTI', 'CALLYBARATION',
    'CALLYBARBAR', 'CALLYBARCALLY', 'CALLYBAREING', 'CALLYBARESE',
    'CALLYBAROUGHT', 'CALLYBARPRES', 'CALLYBARPRI', 'CALLYCALLYABLE',
    'CALLYCALLYANTI', 'CALLYCALLYATION', 'CALLYCALLYBAR', 'CALLYCALLYCALLY',
    'CALLYCALLYEING', 'CALLYCALLYESE', 'CALLYCALLYOUGHT', 'CALLYCALLYPRES',
    'CALLYCALLYPRI', 'CALLYEINGABLE', 'CALLYEINGANTI', 'CALLYEINGATION',
    'CALLYEINGBAR', 'CALLYEINGCALLY', 'CALLYEINGEING', 'CALLYEINGESE',
    'CALLYEINGOUGHT', 'CALLYEINGPRES', 'CALLYEINGPRI', 'CALLYESEABLE',
    'CALLYESEANTI', 'CALLYESEATION', 'CALLYESEBAR', 'CALLYESECALLY',
    'CALLYESEEING', 'CALLYESEESE', 'CALLYESEOUGHT', 'CALLYESEPRES',
    'CALLYESEPRI', 'CALLYOUGHTABLE', 'CALLYOUGHTANTI', 'CALLYOUGHTATION',
    'CALLYOUGHTBAR', 'CALLYOUGHTCALLY', 'CALLYOUGHTEING', 'CALLYOUGHTESE',
    'CALLYOUGHTOUGHT', 'CALLYOUGHTPRES', 'CALLYOUGHTPRI', 'CALLYPRESABLE',
    'CALLYPRESANTI', 'CALLYPRESATION', 'CALLYPRESBAR', 'CALLYPRESCALLY',
    'CALLYPRESEING', 'CALLYPRESESE', 'CALLYPRESOUGHT', 'CALLYPRESPRES',
    'CALLYPRESPRI', 'CALLYPRIABLE', 'CALLYPRIANTI', 'CALLYPRIATION',
    'CALLYPRIBAR', 'CALLYPRICALLY', 'CALLYPRIEING', 'CALLYPRIESE',
    'CALLYPRIOUGHT', 'CALLYPRIPRES', 'CALLYPRIPRI', 'EINGABLEABLE',
    'EINGABLEANTI', 'EINGABLEATION', 'EINGABLEBAR', 'EINGABLECALLY',
    'EINGABLEEING', 'EINGABLEESE', 'EINGABLEOUGHT', 'EINGABLEPRES',
    'EINGABLEPRI', 'EINGANTIABLE', 'EINGANTIANTI', 'EINGANTIATION',
    'EINGANTIBAR', 'EINGANTICALLY', 'EINGANTIEING', 'EINGANTIESE',
    'EINGANTIOUGHT', 'EINGANTIPRES', 'EINGANTIPRI', 'EINGATIONABLE',
    'EINGATIONANTI', 'EINGATIONATION', 'EINGATIONBAR', 'EINGATIONCALLY',
    'EINGATIONEING', 'EINGATIONESE', 'EINGATIONOUGHT', 'EINGATIONPRES',
    'EINGATIONPRI', 'EINGBARABLE', 'EINGBARANTI', 'EINGBARATION',
    'EINGBARBAR', 'EINGBARCALLY', 'EINGBAREING', 'EINGBARESE',
    'EINGBAROUGHT', 'EINGBARPRES', 'EINGBARPRI', 'EINGCALLYABLE',
    'EINGCALLYANTI', 'EINGCALLYATION', 'EINGCALLYBAR', 'EINGCALLYCALLY',
    'EINGCALLYEING', 'EINGCALLYESE', 'EINGCALLYOUGHT', 'EINGCALLYPRES',
    'EINGCALLYPRI', 'EINGEINGABLE', 'EINGEINGANTI', 'EINGEINGATION',
    'EINGEINGBAR', 'EINGEINGCALLY', 'EINGEINGEING', 'EINGEINGESE',
    'EINGEINGOUGHT', 'EINGEINGPRES', 'EINGEINGPRI', 'EINGESEABLE',
    'EINGESEANTI', 'EINGESEATION', 'EINGESEBAR', 'EINGESECALLY',
    'EINGESEEING', 'EINGESEESE', 'EINGESEOUGHT', 'EINGESEPRES',
    'EINGESEPRI', 'EINGOUGHTABLE', 'EINGOUGHTANTI', 'EINGOUGHTATION',
    'EINGOUGHTBAR', 'EINGOUGHTCALLY', 'EINGOUGHTEING', 'EINGOUGHTESE',
    'EINGOUGHTOUGHT', 'EINGOUGHTPRES', 'EINGOUGHTPRI', 'EINGPRESABLE',
    'EINGPRESANTI', 'EINGPRESATION', 'EINGPRESBAR', 'EINGPRESCALLY',
    'EINGPRESEING', 'EINGPRESESE', 'EINGPRESOUGHT', 'EINGPRESPRES',
    'EINGPRESPRI', 'EINGPRIABLE', 'EINGPRIANTI', 'EINGPRIATION',
    'EINGPRIBAR', 'EINGPRICALLY', 'EINGPRIEING', 'EINGPRIESE', 'EINGPRIOUGHT',
    'EINGPRIPRES', 'EINGPRIPRI', 'ESEABLEABLE', 'ESEABLEANTI', 'ESEABLEATION',
    'ESEABLEBAR', 'ESEABLECALLY', 'ESEABLEEING', 'ESEABLEESE', 'ESEABLEOUGHT',
    'ESEABLEPRES', 'ESEABLEPRI', 'ESEANTIABLE', 'ESEANTIANTI', 'ESEANTIATION',
    'ESEANTIBAR', 'ESEANTICALLY', 'ESEANTIEING', 'ESEANTIESE', 'ESEANTIOUGHT',
    'ESEANTIPRES', 'ESEANTIPRI', 'ESEATIONABLE', 'ESEATIONANTI',
    'ESEATIONATION', 'ESEATIONBAR', 'ESEATIONCALLY', 'ESEATIONEING',
    'ESEATIONESE', 'ESEATIONOUGHT', 'ESEATIONPRES', 'ESEATIONPRI',
    'ESEBARABLE', 'ESEBARANTI', 'ESEBARATION', 'ESEBARBAR', 'ESEBARCALLY',
    'ESEBAREING', 'ESEBARESE', 'ESEBAROUGHT', 'ESEBARPRES', 'ESEBARPRI',
    'ESECALLYABLE', 'ESECALLYANTI', 'ESECALLYATION', 'ESECALLYBAR',
    'ESECALLYCALLY', 'ESECALLYEING', 'ESECALLYESE', 'ESECALLYOUGHT',
    'ESECALLYPRES', 'ESECALLYPRI', 'ESEEINGABLE', 'ESEEINGANTI',
    'ESEEINGATION', 'ESEEINGBAR', 'ESEEINGCALLY', 'ESEEINGEING', 'ESEEINGESE',
    'ESEEINGOUGHT', 'ESEEINGPRES', 'ESEEINGPRI', 'ESEESEABLE', 'ESEESEANTI',
    'ESEESEATION', 'ESEESEBAR', 'ESEESECALLY', 'ESEESEEING', 'ESEESEESE',
    'ESEESEOUGHT', 'ESEESEPRES', 'ESEESEPRI', 'ESEOUGHTABLE',
    'ESEOUGHTANTI', 'ESEOUGHTATION', 'ESEOUGHTBAR', 'ESEOUGHTCALLY',
    'ESEOUGHTEING', 'ESEOUGHTESE', 'ESEOUGHTOUGHT', 'ESEOUGHTPRES',
    'ESEOUGHTPRI', 'ESEPRESABLE', 'ESEPRESANTI', 'ESEPRESATION',
    'ESEPRESBAR', 'ESEPRESCALLY', 'ESEPRESEING', 'ESEPRESESE',
    'ESEPRESOUGHT', 'ESEPRESPRES', 'ESEPRESPRI', 'ESEPRIABLE',
    'ESEPRIANTI', 'ESEPRIATION', 'ESEPRIBAR', 'ESEPRICALLY', 'ESEPRIEING',
    'ESEPRIESE', 'ESEPRIOUGHT', 'ESEPRIPRES', 'ESEPRIPRI', 'OUGHTABLEABLE',
    'OUGHTABLEANTI', 'OUGHTABLEATION', 'OUGHTABLEBAR', 'OUGHTABLECALLY',
    'OUGHTABLEEING', 'OUGHTABLEESE', 'OUGHTABLEOUGHT', 'OUGHTABLEPRES',
    'OUGHTABLEPRI', 'OUGHTANTIABLE', 'OUGHTANTIANTI', 'OUGHTANTIATION',
    'OUGHTANTIBAR', 'OUGHTANTICALLY', 'OUGHTANTIEING', 'OUGHTANTIESE',
    'OUGHTANTIOUGHT', 'OUGHTANTIPRES', 'OUGHTANTIPRI', 'OUGHTATIONABLE',
    'OUGHTATIONANTI', 'OUGHTATIONATION', 'OUGHTATIONBAR', 'OUGHTATIONCALLY',
    'OUGHTATIONEING', 'OUGHTATIONESE', 'OUGHTATIONOUGHT', 'OUGHTATIONPRES',
    'OUGHTATIONPRI', 'OUGHTBARABLE', 'OUGHTBARANTI', 'OUGHTBARATION',
    'OUGHTBARBAR', 'OUGHTBARCALLY', 'OUGHTBAREING', 'OUGHTBARESE',
    'OUGHTBAROUGHT', 'OUGHTBARPRES', 'OUGHTBARPRI', 'OUGHTCALLYABLE',
    'OUGHTCALLYANTI', 'OUGHTCALLYATION', 'OUGHTCALLYBAR', 'OUGHTCALLYCALLY',
    'OUGHTCALLYEING', 'OUGHTCALLYESE', 'OUGHTCALLYOUGHT', 'OUGHTCALLYPRES',
    'OUGHTCALLYPRI', 'OUGHTEINGABLE', 'OUGHTEINGANTI', 'OUGHTEINGATION',
    'OUGHTEINGBAR', 'OUGHTEINGCALLY', 'OUGHTEINGEING', 'OUGHTEINGESE',
    'OUGHTEINGOUGHT', 'OUGHTEINGPRES', 'OUGHTEINGPRI', 'OUGHTESEABLE',
    'OUGHTESEANTI', 'OUGHTESEATION', 'OUGHTESEBAR', 'OUGHTESECALLY',
    'OUGHTESEEING', 'OUGHTESEESE', 'OUGHTESEOUGHT', 'OUGHTESEPRES',
    'OUGHTESEPRI', 'OUGHTOUGHTABLE', 'OUGHTOUGHTANTI', 'OUGHTOUGHTATION',
    'OUGHTOUGHTBAR', 'OUGHTOUGHTCALLY', 'OUGHTOUGHTEING', 'OUGHTOUGHTESE',
    'OUGHTOUGHTOUGHT', 'OUGHTOUGHTPRES', 'OUGHTOUGHTPRI', 'OUGHTPRESABLE',
    'OUGHTPRESANTI', 'OUGHTPRESATION', 'OUGHTPRESBAR', 'OUGHTPRESCALLY',
    'OUGHTPRESEING', 'OUGHTPRESESE', 'OUGHTPRESOUGHT', 'OUGHTPRESPRES',
    'OUGHTPRESPRI', 'OUGHTPRIABLE', 'OUGHTPRIANTI', 'OUGHTPRIATION',
    'OUGHTPRIBAR', 'OUGHTPRICALLY', 'OUGHTPRIEING', 'OUGHTPRIESE',
    'OUGHTPRIOUGHT', 'OUGHTPRIPRES', 'OUGHTPRIPRI', 'PRESABLEABLE',
    'PRESABLEANTI', 'PRESABLEATION', 'PRESABLEBAR', 'PRESABLECALLY',
    'PRESABLEEING', 'PRESABLEESE', 'PRESABLEOUGHT', 'PRESABLEPRES',
    'PRESABLEPRI', 'PRESANTIABLE', 'PRESANTIANTI', 'PRESANTIATION',
    'PRESANTIBAR', 'PRESANTICALLY', 'PRESANTIEING', 'PRESANTIESE',
    'PRESANTIOUGHT', 'PRESANTIPRES', 'PRESANTIPRI', 'PRESATIONABLE',
    'PRESATIONANTI', 'PRESATIONATION', 'PRESATIONBAR', 'PRESATIONCALLY',
    'PRESATIONEING', 'PRESATIONESE', 'PRESATIONOUGHT', 'PRESATIONPRES',
    'PRESATIONPRI', 'PRESBARABLE', 'PRESBARANTI', 'PRESBARATION',
    'PRESBARBAR', 'PRESBARCALLY', 'PRESBAREING', 'PRESBARESE',
    'PRESBAROUGHT', 'PRESBARPRES', 'PRESBARPRI', 'PRESCALLYABLE',
    'PRESCALLYANTI', 'PRESCALLYATION', 'PRESCALLYBAR', 'PRESCALLYCALLY',
    'PRESCALLYEING', 'PRESCALLYESE', 'PRESCALLYOUGHT', 'PRESCALLYPRES',
    'PRESCALLYPRI', 'PRESEINGABLE', 'PRESEINGANTI', 'PRESEINGATION',
    'PRESEINGBAR', 'PRESEINGCALLY', 'PRESEINGEING', 'PRESEINGESE',
    'PRESEINGOUGHT', 'PRESEINGPRES', 'PRESEINGPRI', 'PRESESEABLE',
    'PRESESEANTI', 'PRESESEATION', 'PRESESEBAR', 'PRESESECALLY',
    'PRESESEEING', 'PRESESEESE', 'PRESESEOUGHT', 'PRESESEPRES',
    'PRESESEPRI', 'PRESOUGHTABLE', 'PRESOUGHTANTI', 'PRESOUGHTATION',
    'PRESOUGHTBAR', 'PRESOUGHTCALLY', 'PRESOUGHTEING', 'PRESOUGHTESE',
    'PRESOUGHTOUGHT', 'PRESOUGHTPRES', 'PRESOUGHTPRI', 'PRESPRESABLE',
    'PRESPRESANTI', 'PRESPRESATION', 'PRESPRESBAR', 'PRESPRESCALLY',
    'PRESPRESEING', 'PRESPRESESE', 'PRESPRESOUGHT', 'PRESPRESPRES',
    'PRESPRESPRI', 'PRESPRIABLE', 'PRESPRIANTI', 'PRESPRIATION',
    'PRESPRIBAR', 'PRESPRICALLY', 'PRESPRIEING', 'PRESPRIESE', 'PRESPRIOUGHT',
    'PRESPRIPRES', 'PRESPRIPRI', 'PRIABLEABLE', 'PRIABLEANTI', 'PRIABLEATION',
    'PRIABLEBAR', 'PRIABLECALLY', 'PRIABLEEING', 'PRIABLEESE', 'PRIABLEOUGHT',
    'PRIABLEPRES', 'PRIABLEPRI', 'PRIANTIABLE', 'PRIANTIANTI', 'PRIANTIATION',
    'PRIANTIBAR', 'PRIANTICALLY', 'PRIANTIEING', 'PRIANTIESE', 'PRIANTIOUGHT',
    'PRIANTIPRES', 'PRIANTIPRI', 'PRIATIONABLE', 'PRIATIONANTI',
    'PRIATIONATION', 'PRIATIONBAR', 'PRIATIONCALLY', 'PRIATIONEING',
    'PRIATIONESE', 'PRIATIONOUGHT', 'PRIATIONPRES', 'PRIATIONPRI',
    'PRIBARABLE', 'PRIBARANTI', 'PRIBARATION', 'PRIBARBAR', 'PRIBARCALLY',
    'PRIBAREING', 'PRIBARESE', 'PRIBAROUGHT', 'PRIBARPRES', 'PRIBARPRI',
    'PRICALLYABLE', 'PRICALLYANTI', 'PRICALLYATION', 'PRICALLYBAR',
    'PRICALLYCALLY', 'PRICALLYEING', 'PRICALLYESE', 'PRICALLYOUGHT',
    'PRICALLYPRES', 'PRICALLYPRI', 'PRIEINGABLE', 'PRIEINGANTI',
    'PRIEINGATION', 'PRIEINGBAR', 'PRIEINGCALLY', 'PRIEINGEING', 'PRIEINGESE',
    'PRIEINGOUGHT', 'PRIEINGPRES', 'PRIEINGPRI', 'PRIESEABLE', 'PRIESEANTI',
    'PRIESEATION', 'PRIESEBAR', 'PRIESECALLY', 'PRIESEEING', 'PRIESEESE',
    'PRIESEOUGHT', 'PRIESEPRES', 'PRIESEPRI', 'PRIOUGHTABLE', 'PRIOUGHTANTI',
    'PRIOUGHTATION', 'PRIOUGHTBAR', 'PRIOUGHTCALLY', 'PRIOUGHTEING',
    'PRIOUGHTESE', 'PRIOUGHTOUGHT', 'PRIOUGHTPRES', 'PRIOUGHTPRI',
    'PRIPRESABLE', 'PRIPRESANTI', 'PRIPRESATION', 'PRIPRESBAR',
    'PRIPRESCALLY', 'PRIPRESEING', 'PRIPRESESE', 'PRIPRESOUGHT',
    'PRIPRESPRES', 'PRIPRESPRI', 'PRIPRIABLE', 'PRIPRIANTI', 'PRIPRIATION',
    'PRIPRIBAR', 'PRIPRICALLY', 'PRIPRIEING', 'PRIPRIESE', 'PRIPRIOUGHT',
    'PRIPRIPRES', 'PRIPRIPRI' ]
        
    lastname = allnames[random.randint(0,len(allnames)-1)]
    return lastname
