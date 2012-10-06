package com.wwm.db.server.whirlwind;

import org.fuzzydb.server.internal.index.IndexImplementation;
import org.fuzzydb.server.services.IndexImplementationsService;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;


public class Activator implements BundleActivator {

	private ServiceTracker indexImplsSvcTracker;
	private IndexImplementation wwIndexImpl = new WhirlwindIndexImpl();
	
	/*
	 * (non-Javadoc)
	 * @see org.osgi.framework.BundleActivator#start(org.osgi.framework.BundleContext)
	 */
	public void start(BundleContext context) throws Exception {
		
		
		// create a tracker and track the service, and get callback when this service is added
		indexImplsSvcTracker = new ServiceTracker(context, IndexImplementationsService.class.getName(), null){
			@Override
			public Object addingService(ServiceReference reference) {
				// FIXME: This is where we need to register the WW index impl with the indexes impl
				
				IndexImplementationsService svc = (IndexImplementationsService) context.getService(reference);
				svc.add( wwIndexImpl );
				
				System.out.println("Index Svc Added");
				return super.addingService(reference);
			}
			
			@Override
			public void removedService(ServiceReference reference, Object service) {
				super.removedService(reference, service);
				
				// FIXME: This is where we need to remove the WW index impl with the indexes impl
				
				IndexImplementationsService svc = (IndexImplementationsService) context.getService(reference);
				svc.remove( wwIndexImpl );
			}
		};
		indexImplsSvcTracker.open();
		
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
	 */
	public void stop(BundleContext context) throws Exception {
		// close the service tracker
		indexImplsSvcTracker.close();
		indexImplsSvcTracker = null;
		
	}
}
