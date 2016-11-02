
    def clean_dtimes( self,
                      objreader = {} ):
        # Rename into shorter aliases
        return True
        o2g = self.obj2group
        oir = objreader
        counter = Counter()
        # counter key is number of death times in a group
        #         value is number of unique death times in a group
        # There is a bug in the C++ simulator where groups of different death times are merged.
        # It was easier to clean up here because we know the death times are accurate.
        # The more conscientious thing to do would be to fix the bug in the simulator.
        dtimes = {}
        newgroup = defaultdict(set)
        dtime2group = {}
        for gnum in self.group2list.keys():
            origdtime = self.group2dtime[gnum]
            dtimes[gnum] = set( [ oir.get_death_time(x) for x in self.group2list[gnum] ] )
            dtime2group[origdtime] = gnum
        pp.pprint( self.group2dtime )
        exit(100)
        for gnum in self.group2list.keys():
            origdtime = self.group2dtime[gnum]
            if len(dtimes[gnum]) > 1:
                counter[len(dtimes[gnum])] += 1
                # Clean up aisle greater than 1. (Bad joke).
                # We will let the original group number keep the dtime that has been
                # assigned in group2dtime.
                for objId in self.group2list[gnum]:
                    dt = oir.get_death_time(objId)
                    if dt != origdtime:
                        # We need to either assign to an existing group that has
                        # the same dtime, or create a new group. But we'll do that later
                        newgroup[dt].add(objId)
                        # 1- Remove from group2list
                        self.group2list[gnum].remove( objId )
                        # 2- Remove from obj2group
                        if objId in self.obj2group:
                            del self.obj2group[objId]
                    # else:
                    #     # Object belongs to original group.
                    #     # There's no need to move groups.
                    #     pass
                # What needs to be adjusted if we split the group?
                # group2dtime
                # group2list
                # obj2group
            else:
                counter[1] += 1
                newdtime = list(dtimes[gnum])[0]
                if newdtime != origdtime:
                    print "ERROR: Group num[ %d ] dtimes do not match  %d != %d" % \
                        (gnum, newdtime, origdtime)
                    self.logger.error( "Group num[ %d ] dtimes do not match  %d != %d" %
                                       (gnum, newdtime, origdtime) )
        # Remove empty lists in group2list
        for gnum in self.group2list.keys():
            if len(self.group2list[gnum]) == 0:
                del self.group2list[gnum]
                if gnum in self.group2dtime:
                    del self.group2dtime[gnum]
                print "DEBUG: Group number %d removed" % gnum
                self.logger.error( "Group number %d removed" % gnum )
        # Get the largest groupnumber in group2list
        last_gnum = max( self.group2list.keys() )
        # Get the newgroup dictionary and reassign if needed
        for dt, myset in newgroup.iteritems():
            # Get the group number based on death time
            if dt in dtime2group:
                gnum = dtime2group[dt]
            else:
                gnum = last_gnum + 1
                last_gnum = gnum
                dtime2group[dt] = gnum
            # Add 'myset' to group2list
            if gnum in self.group2list:
                self.group2list[gnum].extend(list(myset))
            else:
                self.group2list[gnum] = list(myset)
            # Go through the new additions from 'myset' and set the proper obj2group
            for objId in self.group2list[gnum]:
                self.obj2group[objId] = [ gnum ]
            # Set the proper death time for the group
            self.group2dtime[gnum] = dt
        # DEBUG statements. Keeping it here just in case. -RLV
        # print "=======[ CLEAN DEBUG ]=========================================================="
        # pp.pprint(counter)
        # print "--------------------------------------------------------------------------------"
        # pp.pprint(newgroup)
        # print "NEW MAX", last_gnum
        # print "=======[ END CLEAN DEBUG ]======================================================"
        return (len(counter) == 1)

    def merge_groups_with_same_dtime( self,
                                      objreader = {},
                                      verify = False ):
        # Rename into shorter aliases
        oir = objreader
        g2d = self.group2dtime
        counter = Counter()
        dtime2group = defaultdict(set)
        for gnum in g2d.keys():
            dt = self.group2dtime[gnum]
            dtime2group[dt].add(gnum)
        # Start with known groups
        new_dtime2group = {}
        for dtime, gset in dtime2group.iteritems():
            if len(gset) > 1:
                # Merge into the lower group number
                # Sort the set into increasing group numbers
                glist = sorted( list(gset) )
                newgnum = glist[0]
                for other in glist[1:]:
                    # Save the list
                    otherlist = self.group2list[other]
                    # Remove it
                    del self.group2list[other]
                    # Add to the target group
                    self.group2list[newgnum].extend(otherlist)
                    # Remove old group number
                    del self.group2dtime[other]
                    # Update the obj2group map
                    for objId in otherlist:
                        self.obj2group[objId] = set([ newgnum ])
            else:
                new_dtime2group[dtime] = list(gset)[0]
        dtime2group = new_dtime2group
        # Next clean the ones who don't belong to a group. Most of these (all?)
        # are "died at end of program" objects.
        # Get the largest known group number and start from there
        last_gnum = max( self.group2list.keys() )
        # Save a group number for all objects that died at end
        atend_gnum = last_gnum + 1
        self._atend_gnum = atend_gnum
        last_gnum = atend_gnum
        for objId in oir.keys():
            if objId not in self.obj2group:
                # A "no death group" object
                dt = oir.get_death_time(objId)
                if dt in dtime2group:
                    # Known death time. Add it there
                    gnum = dtime2group[dt]
                    self.group2list[gnum].append(objId)
                    self.obj2group[objId] = [ gnum ]
                    self.logger.debug( "Adding object [%d] to group [%d]" % (objId, gnum) )
                else:
                    # Alert: new death time. Create a new group.
                    if oir.died_at_end(objId):
                        # Adding to DIED AT END group
                        dtime2group[dt] = atend_gnum
                        if atend_gnum in self.group2list:
                            self.group2list[atend_gnum].append( objId )
                        else:
                            self.group2list[atend_gnum] = [ objId ]
                        self.obj2group[objId] = [ atend_gnum ]
                        self.logger.debug( "Adding object [%d] to AT END group [%d]" % (objId, atend_gnum) )
                    else:
                        # Add to a new group
                        gnum = last_gnum + 1
                        last_gnum = gnum
                        dtime2group[dt] = gnum
                        self.group2list[gnum] = [ objId ]
                        self.obj2group[objId] = [ gnum ]
                        self.logger.debug( "Adding object [%d] to group [%d]" % (objId, gnum) )
        # Do we need to verify?
        if verify:
            dtime2group = defaultdict(set)
            for gnum in g2d.keys():
                dt = self.group2dtime[gnum]
                dtime2group[dt].add(gnum)
            errorflag = False
            for dtime, gnumset in dtime2group.iteritems():
                if len(gnumset) > 1:
                    errorflag = True
                    print "Merge NOT successful -> group [%d] => %d" % (gnumset, len(gnumset))
                    self.logger.critical( "Merge NOT successful -> group [%d] => %d" % (gnumset, len(gnumset)) )
            if errorflag:
                print "EXITING."
                exit(1)


        #----------------------------------------------------------------------
        # TODO: Maybe we don't need the Sqlite DB for this.
        if False:
            self.dbfilename = dbfilename
            self.create_dgroup_db( outdbfilename = dbfilename )
            # Sort group2list according to size. (Largest first.)
            def keyfn( tup ):
                return len(tup[1])
            newgrouplist = sorted( self.group2list.iteritems(),
                                   key = keyfn,
                                   reverse = True )
            exit(100) # TODO HERE TODO HERE TODO
            # Declare our generator
            # ----------------------------------------------------------------------
            oir = object_info_reader # Use a shorter alias
            def row_generator():
                start = False
                count = 0
                for line in fp:
                    count += 1
                    line = line.rstrip()
                    if line.find("---------------[ CYCLES") == 0:
                        start = True if not start else False
                        if start:
                            continue
                        else:
                            break
                    if start:
                        line = line.rstrip()
                        line = line.rstrip(",")
                        # Remove all objects that died at program end.
                        dg = [ int(x) for x in line.split(",") if not oir.died_at_end(int(x))  ]
                        if len(dg) == 0:
                            continue
                        dtimes = list( set( [ oir.get_death_time(x) for x in dg ] ) )

            # ----------------------------------------------------------------------
            # TODO call executemany here
            cur = self.outdbconn.cursor()
            cur.executemany( "INSERT INTO objinfo VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row_generator() )
            cur.executemany( "INSERT INTO typetable VALUES (?,?)", type_row_generator() )
            cur.execute( 'CREATE UNIQUE INDEX idx_objectinfo_objid ON objinfo (objid)' )
            cur.execute( 'CREATE UNIQUE INDEX idx_typeinfo_typeid ON typetable (typeid)' )
            self.outdbconn.commit()
            self.outdbconn.close()

