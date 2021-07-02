package deployer

import (
	fleet "github.com/rancher/fleet/pkg/apis/fleet.cattle.io/v1alpha1"
	fleetcontrollers "github.com/rancher/fleet/pkg/generated/controllers/fleet.cattle.io/v1alpha1"
	"github.com/rancher/fleet/pkg/manifest"
	"github.com/rancher/wrangler/pkg/apply"
	"github.com/rancher/wrangler/pkg/kv"
	"github.com/sirupsen/logrus"
	apierror "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type Manager struct {
	fleetNamespace        string
	defaultNamespace      string
	bundleDeploymentCache fleetcontrollers.BundleDeploymentCache
	lookup                manifest.Lookup
	deployer              Deployer
	apply                 apply.Apply
	labelPrefix           string
}

func NewManager(fleetNamespace string,
	defaultNamespace string,
	labelPrefix string,
	bundleDeploymentCache fleetcontrollers.BundleDeploymentCache,
	lookup manifest.Lookup,
	deployer Deployer,
	apply apply.Apply) *Manager {
	return &Manager{
		fleetNamespace:        fleetNamespace,
		defaultNamespace:      defaultNamespace,
		labelPrefix:           labelPrefix,
		bundleDeploymentCache: bundleDeploymentCache,
		lookup:                lookup,
		deployer:              deployer,
		apply:                 apply.WithDynamicLookup(),
	}
}

func (m *Manager) releaseName(bd *fleet.BundleDeployment) string {
	ns := m.defaultNamespace
	if bd.Spec.Options.TargetNamespace != "" {
		ns = bd.Spec.Options.TargetNamespace
	} else if bd.Spec.Options.DefaultNamespace != "" {
		ns = bd.Spec.Options.DefaultNamespace
	}
	if bd.Spec.Options.Helm == nil || bd.Spec.Options.Helm.ReleaseName == "" {
		return ns + "/" + bd.Name
	}
	return ns + "/" + bd.Spec.Options.Helm.ReleaseName
}

func (m *Manager) Cleanup() error {
	logrus.Info("Calling Cleanup() in deployer manager")
	deployed, err := m.deployer.ListDeployments()
	if err != nil {
		logrus.Error(err)
		return err
	}

	for _, deployed := range deployed {
		bundleDeployment, err := m.bundleDeploymentCache.Get(m.fleetNamespace, deployed.BundleID)
		if apierror.IsNotFound(err) {
			logrus.Infof("Deleting orphan bundle ID %s, release %s", deployed.BundleID, deployed.ReleaseName)
			if err := m.deployer.Delete(deployed.BundleID, deployed.ReleaseName); err != nil {
				logrus.Error(err)
				return err
			}
		} else if err != nil {
			logrus.Error(err)
			return err
		} else {
			releaseName := m.releaseName(bundleDeployment)
			if releaseName != deployed.ReleaseName {
				logrus.Infof("Deleting unknown bundle ID %s, release %s, expecting release %s", deployed.BundleID, deployed.ReleaseName, releaseName)
				if err := m.deployer.Delete(deployed.BundleID, deployed.ReleaseName); err != nil {
					logrus.Error(err)
					return err
				}
			}
		}
	}

	return nil
}

func (m *Manager) Delete(bundleDeploymentKey string) error {
	_, name := kv.RSplit(bundleDeploymentKey, "/")
	return m.deployer.Delete(name, "")
}

func (m *Manager) Resources(bd *fleet.BundleDeployment) (*Resources, error) {
	resources, err := m.deployer.Resources(bd.Name, bd.Status.Release)
	if err != nil {
		logrus.Infof("No error from calling deploy with resources from manager for bundle %s", bd.Name)
		return nil, nil
	}

	plan, err := m.plan(bd, resources.DefaultNamespace, resources.Objects...)
	if err != nil {
		logrus.Errorf("Error calling plan in manager, %s", err)
		return nil, err
	}

	for gvk, keys := range plan.Delete {
		for _, key := range keys {
			u := &unstructured.Unstructured{}
			u.SetGroupVersionKind(gvk)
			u.SetNamespace(key.Namespace)
			u.SetName(key.Name)
			resources.Objects = append(resources.Objects, u)
		}
	}

	logrus.Infof("Resources for bundle %s, %+v", bd.Name, resources)
	return resources, nil
}

func (m *Manager) Deploy(bd *fleet.BundleDeployment) (string, error) {
	if bd.Spec.DeploymentID == bd.Status.AppliedDeploymentID {
		if ok, err := m.deployer.EnsureInstalled(bd.Name, bd.Status.Release); err != nil {
			logrus.Errorf("Error calling EnsureInstalled from Deploy manager: %s", err)
			return "", err
		} else if ok {
			logrus.Infof("EnsureInstalled in Deploy manager is ok, release status: %+v", bd.Status.Release)
			return bd.Status.Release, nil
		}
	}

	manifestID, _ := kv.Split(bd.Spec.DeploymentID, ":")
	manifest, err := m.lookup.Get(manifestID)
	if err != nil {
		logrus.Errorf("Error looking up manifest: %s - %s", manifestID, err)
		return "", err
	}

	manifest.Commit = bd.Labels["fleet.cattle.io/commit"]
	resource, err := m.deployer.Deploy(bd.Name, manifest, bd.Spec.Options)
	if err != nil {
		logrus.Errorf("Error from manager deploying %s", err)
		return "", err
	}

	logrus.Infof("Successfully deployed resource: %s", resource.ID)
	return resource.ID, nil
}
